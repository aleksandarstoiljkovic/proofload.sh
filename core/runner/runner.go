// Package runner implements proofload's core load loop: a concurrent,
// coordinated-omission-correct driver of database operations across many
// connections. Each run has a discarded warmup phase followed by a measured
// phase; both pace off a single shared scheduler and one monotonic clock base.
//
// Latency is always measured from the operation's INTENDED start time (not the
// moment it is dispatched), so a client that falls behind the requested arrival
// rate shows up as growing latency rather than being silently hidden — this is
// the coordinated-omission correction. When mean dispatch lateness exceeds the
// lateness threshold, the run is flagged ClientBound: the load generator, not
// the target, was the bottleneck and the target numbers are not trustworthy.
package runner

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// OpGen produces the next operation for one connection. One OpGen is built per
// connection, so implementations need not be safe for concurrent use.
type OpGen interface {
	Next() domain.Operation
}

// Scheduler yields the intended start time of the next operation. It is shared
// by every connection, so implementations MUST be safe for concurrent use. A
// returned time in the past means "fire now" (closed-loop / saturation).
type Scheduler interface {
	Next() time.Time
}

// Sink records the outcome of one measured operation. One Sink is built per
// connection, so implementations need not be safe for concurrent use.
type Sink interface {
	Record(op domain.OpType, latency time.Duration, isErr bool)
}

// OpObserver receives every MEASURE-phase operation with its full context: the
// concrete operation, the driver result, and the real-time invoke/complete
// instants (dispatch and completion). It powers correctness recording — the
// reconciliation expectation log and the Elle/Porcupine operation history —
// without the runner depending on those packages. One OpObserver is built per
// connection, so implementations need not be safe for concurrent use.
type OpObserver interface {
	Observe(op domain.Operation, res domain.OpResult, invoke, complete time.Time)
}

// Deps are the collaborators the load loop drives. Gen and Sink are builders so
// the runner can hand each connection its own generator and sink. Sink is
// nil-safe: a nil Sink func, or one that returns nil, yields a discard sink
// (also used for every warmup operation).
type Deps struct {
	Driver   driver.Driver
	Cfg      driver.Config
	Workload domain.Workload
	Sched    Scheduler
	Gen      func(shard int) OpGen      // build a per-connection generator
	Sink     func(shard int) Sink       // build a per-connection sink (nil-safe)
	Observer func(shard int) OpObserver // optional per-connection observer (nil-safe)
}

// Options configures one run.
type Options struct {
	Connections int
	Warmup      time.Duration
	Duration    time.Duration
	// LatenessThreshold: if the mean dispatch lateness exceeds this, the client
	// (not the target) was the bottleneck. Default (<= 0) = one mean
	// inter-arrival, computed from the intended arrival times of the run.
	LatenessThreshold time.Duration
}

// RunReport summarizes the MEASURE phase only.
type RunReport struct {
	Total, Errors int64
	Records       int64 // records touched by successful ops (= Total unless batch ops set Rows>1)
	ClientBound   bool
	MeanLateness  time.Duration
	MeanService   time.Duration // mean per-op service time (completion - dispatch)
	Duration      time.Duration
}

// Run opens Connections connections, prepares each for the workload, then drives
// load through a warmup phase (discarded) and a measure phase (recorded). It
// returns when the measure phase elapses or ctx is cancelled; connections are
// always closed before returning. On cancellation it returns the partial report
// together with ctx.Err().
func Run(ctx context.Context, d Deps, o Options) (RunReport, error) {
	if err := validate(d, o); err != nil {
		return RunReport{}, err
	}
	conns, err := connectAll(ctx, d, o.Connections)
	if err != nil {
		return RunReport{}, err
	}
	defer closeAll(conns)

	base := time.Now()
	warmupEnd := base.Add(o.Warmup)
	measureEnd := warmupEnd.Add(o.Duration)

	runCtx, cancel := context.WithDeadline(ctx, measureEnd)
	defer cancel()

	r := &runState{
		deps:      d,
		warmupEnd: warmupEnd,
		acc:       &accumulator{minIntended: math.MaxInt64, maxIntended: math.MinInt64},
	}
	r.wg.Add(o.Connections)
	for i, c := range conns {
		go r.worker(runCtx, i, c)
	}
	r.wg.Wait()

	report := r.acc.report(warmupEnd, o)
	if err := ctx.Err(); err != nil {
		return report, err
	}
	return report, nil
}

func validate(d Deps, o Options) error {
	switch {
	case d.Driver == nil:
		return fmt.Errorf("runner: nil Driver")
	case d.Sched == nil:
		return fmt.Errorf("runner: nil Scheduler")
	case d.Gen == nil:
		return fmt.Errorf("runner: nil Gen")
	case o.Connections < 1:
		return fmt.Errorf("runner: Connections must be >= 1, got %d", o.Connections)
	case o.Duration <= 0:
		return fmt.Errorf("runner: Duration must be > 0, got %v", o.Duration)
	case o.Warmup < 0:
		return fmt.Errorf("runner: Warmup must be >= 0, got %v", o.Warmup)
	}
	return nil
}

// connectAll opens and prepares n connections, failing fast and closing any it
// already opened if Connect or Prepare returns an error.
func connectAll(ctx context.Context, d Deps, n int) ([]driver.Conn, error) {
	conns := make([]driver.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := d.Driver.Connect(ctx, d.Cfg)
		if err != nil {
			closeAll(conns)
			return nil, fmt.Errorf("runner: connect %d: %w", i, err)
		}
		if err := c.Prepare(ctx, d.Workload); err != nil {
			_ = c.Close()
			closeAll(conns)
			return nil, fmt.Errorf("runner: prepare %d: %w", i, err)
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func closeAll(conns []driver.Conn) {
	for _, c := range conns {
		_ = c.Close()
	}
}
