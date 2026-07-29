package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// accumulator holds the measure-phase tallies, updated concurrently by workers
// via atomics only.
type accumulator struct {
	total       int64 // measured operations
	errors      int64 // measured operations whose OpResult.Err != nil
	latenessSum int64 // sum of per-op dispatch lateness, nanoseconds
	serviceSum  int64 // sum of per-op service time (done-dispatch), nanoseconds
	records     int64 // sum of OpResult.Rows — records touched (>ops for batch ops)
	minIntended int64 // smallest intended start seen, unix nanoseconds
	maxIntended int64 // largest intended start seen, unix nanoseconds
}

// runState is the shared state for one Run invocation.
type runState struct {
	deps      Deps
	warmupEnd time.Time
	acc       *accumulator
	wg        sync.WaitGroup
}

// worker drives one connection until the run context is done (measure deadline
// reached or parent cancelled). It gets its own generator and sink.
func (r *runState) worker(ctx context.Context, shard int, conn driver.Conn) {
	defer r.wg.Done()
	gen := r.deps.Gen(shard)
	sink := r.measureSink(shard)
	obs := r.measureObserver(shard)
	for {
		if ctx.Err() != nil {
			return
		}
		intended := r.deps.Sched.Next()
		if !sleepUntil(ctx, intended) {
			return
		}
		op := gen.Next()
		dispatch := time.Now()
		res := conn.Execute(ctx, op)
		done := time.Now()
		if ctx.Err() != nil {
			return // interrupted mid-flight: discard this operation
		}
		r.observe(sink, obs, op, res, intended, dispatch, done)
	}
}

// observe classifies a completed operation into its phase and records it.
// Latency is measured from the INTENDED start (coordinated-omission correct);
// warmup operations are discarded.
func (r *runState) observe(sink Sink, obs OpObserver, op domain.Operation, res domain.OpResult, intended, dispatch, done time.Time) {
	if dispatch.Before(r.warmupEnd) {
		return // warmup phase: discarded from the report, the sink, and the observer
	}
	latency := done.Sub(intended)
	isErr := res.Err != nil
	sink.Record(res.Type, latency, isErr)
	if obs != nil {
		obs.Observe(op, res, dispatch, done)
	}

	a := r.acc
	atomic.AddInt64(&a.total, 1)
	if isErr {
		atomic.AddInt64(&a.errors, 1)
	} else {
		rows := res.Rows
		if rows < 1 {
			rows = 1
		}
		atomic.AddInt64(&a.records, int64(rows)) // records touched by successful ops (batch ops touch many)
	}
	lateness := dispatch.Sub(intended)
	if lateness < 0 {
		lateness = 0
	}
	atomic.AddInt64(&a.latenessSum, int64(lateness))
	if svc := done.Sub(dispatch); svc > 0 {
		atomic.AddInt64(&a.serviceSum, int64(svc))
	}
	in := intended.UnixNano()
	atomicMin(&a.minIntended, in)
	atomicMax(&a.maxIntended, in)
}

// measureObserver returns the per-shard observer, or nil when none is built.
func (r *runState) measureObserver(shard int) OpObserver {
	if r.deps.Observer == nil {
		return nil
	}
	return r.deps.Observer(shard)
}

// measureSink returns the per-shard sink, or a discard sink when none is built.
func (r *runState) measureSink(shard int) Sink {
	if r.deps.Sink == nil {
		return discardSink{}
	}
	if s := r.deps.Sink(shard); s != nil {
		return s
	}
	return discardSink{}
}

// sleepUntil blocks until t, honoring ctx. It uses a monotonic-clock deadline
// (time.Until reads the monotonic component of t). A non-future t fires
// immediately. It returns false if ctx is done before t.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// report builds the final RunReport from the measure-phase accumulator.
func (a *accumulator) report(warmupEnd time.Time, o Options) RunReport {
	total := atomic.LoadInt64(&a.total)
	rep := RunReport{
		Total:    total,
		Errors:   atomic.LoadInt64(&a.errors),
		Records:  atomic.LoadInt64(&a.records),
		Duration: measureElapsed(warmupEnd, o.Duration),
	}
	if total == 0 {
		return rep
	}
	rep.MeanLateness = time.Duration(atomic.LoadInt64(&a.latenessSum) / total)
	rep.MeanService = time.Duration(atomic.LoadInt64(&a.serviceSum) / total)
	threshold := o.LatenessThreshold
	if threshold <= 0 {
		threshold = a.meanInterArrival(total)
	}
	// Client-bound means the LOAD GENERATOR — not the target — was the limiter:
	// dispatch fell behind the intended schedule (lateness > one inter-arrival)
	// AND that backlog is not explained by the target being slow (lateness
	// exceeds mean service time). When the target itself is slow, coordinated
	// omission already surfaces it as latency, so we must NOT mislabel that as
	// client-bound (that was the false signal during fault-induced latency
	// spikes). The remedy for a genuinely client-bound run is more connections
	// or horizontal scaling with --workers.
	// A meaningful error rate means the target (or an injected fault) was in
	// trouble, so lateness is not the client's fault — don't flag client-bound.
	lowErrors := rep.Errors*20 < total // < 5% errors
	rep.ClientBound = threshold > 0 &&
		rep.MeanLateness > threshold &&
		rep.MeanLateness > rep.MeanService &&
		lowErrors
	return rep
}

// meanInterArrival estimates the mean gap between intended arrivals across the
// measure phase: (maxIntended - minIntended) / (total - 1).
func (a *accumulator) meanInterArrival(total int64) time.Duration {
	if total < 2 {
		return 0
	}
	span := atomic.LoadInt64(&a.maxIntended) - atomic.LoadInt64(&a.minIntended)
	if span <= 0 {
		return 0
	}
	return time.Duration(span / (total - 1))
}

// measureElapsed reports the wall-clock length of the measure phase, clamped to
// [0, limit].
func measureElapsed(warmupEnd time.Time, limit time.Duration) time.Duration {
	d := time.Since(warmupEnd)
	if d < 0 {
		return 0
	}
	if d > limit {
		return limit
	}
	return d
}

func atomicMin(addr *int64, v int64) {
	for {
		old := atomic.LoadInt64(addr)
		if old <= v {
			return
		}
		if atomic.CompareAndSwapInt64(addr, old, v) {
			return
		}
	}
}

func atomicMax(addr *int64, v int64) {
	for {
		old := atomic.LoadInt64(addr)
		if old >= v {
			return
		}
		if atomic.CompareAndSwapInt64(addr, old, v) {
			return
		}
	}
}

// discardSink drops every record; used for warmup and when no sink is built.
type discardSink struct{}

func (discardSink) Record(domain.OpType, time.Duration, bool) {}
