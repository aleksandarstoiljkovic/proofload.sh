package metrics

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

// Options configures a Recorder. The zero value is valid; unset fields fall
// back to SignificantFigures=3 and MaxLatency=60s.
type Options struct {
	// SignificantFigures is the HdrHistogram precision (1..5). Default 3.
	SignificantFigures int
	// MaxLatency is the largest latency tracked without clamping. Default 60s.
	MaxLatency time.Duration
}

// Recorder aggregates latency histograms across many Locals. The Record hot
// path lives entirely on Local and takes no Recorder lock; the Recorder lock is
// held only for registration and aggregation (Snapshot, TakeInterval, Merge,
// Encode, WriteHLog).
type Recorder struct {
	high int64
	sig  int

	mu     sync.Mutex
	locals []*Local

	// interval state, guarded by mu, for TakeInterval deltas.
	prevSnap map[domain.OpType]*hdrhistogram.Snapshot
	prevErr  map[domain.OpType]int64
}

// New builds a Recorder from Options, applying defaults for unset fields.
func New(o Options) *Recorder {
	sig := o.SignificantFigures
	if sig < 1 || sig > 5 {
		sig = defaultSigFigs
	}
	max := o.MaxLatency
	if max <= 0 {
		max = defaultMaxLatency
	}
	return &Recorder{
		high:     max.Nanoseconds(),
		sig:      sig,
		prevSnap: make(map[domain.OpType]*hdrhistogram.Snapshot),
		prevErr:  make(map[domain.OpType]int64),
	}
}

// Local returns a fresh per-goroutine sink registered with this Recorder. Call
// it once per load goroutine before the hot loop.
func (r *Recorder) Local() *Local {
	l := newLocal(r.high, r.sig)
	r.mu.Lock()
	r.locals = append(r.locals, l)
	r.mu.Unlock()
	return l
}

// merged is a point-in-time cumulative aggregate of every registered Local.
type merged struct {
	overall  *hdrhistogram.Histogram
	byOp     map[domain.OpType]*hdrhistogram.Histogram
	errByOp  map[domain.OpType]int64
	errTotal int64
}

// collect merges all Locals into a fresh aggregate. It holds the Recorder lock
// and briefly each Local's lock, so it is race-free against concurrent Record.
func (r *Recorder) collect() *merged {
	m := &merged{
		overall: newHist(r.high, r.sig),
		byOp:    make(map[domain.OpType]*hdrhistogram.Histogram),
		errByOp: make(map[domain.OpType]int64),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.locals {
		l.mu.Lock()
		m.overall.Merge(l.overall)
		for op, h := range l.byOp {
			dst := m.byOp[op]
			if dst == nil {
				dst = newHist(r.high, r.sig)
				m.byOp[op] = dst
			}
			dst.Merge(h)
		}
		for op, c := range l.errByOp {
			m.errByOp[op] += c
		}
		m.errTotal += l.errTotal
		l.mu.Unlock()
	}
	return m
}

// Snapshot merges all Locals and produces the final aggregate for one phase.
// RunID is left unset (the zero RunID); the caller stamps it at persist time,
// since Snapshot has no run identity to draw from.
func (r *Recorder) Snapshot(phase domain.Phase, dur time.Duration) domain.RunResult {
	m := r.collect()
	res := domain.RunResult{
		Phase:    phase,
		Total:    m.overall.TotalCount(),
		Errors:   m.errTotal,
		Duration: dur,
		Overall:  toPercentiles(m.overall),
		ByOp:     make(map[domain.OpType]domain.Percentiles, len(m.byOp)),
	}
	if dur > 0 {
		res.Throughput = float64(res.Total) / dur.Seconds()
	}
	for op, h := range m.byOp {
		res.ByOp[op] = toPercentiles(h)
	}
	return res
}

// Merge folds another Recorder's histograms into this one losslessly. It reads
// a consistent aggregate of other and installs it as one synthetic Local, so
// subsequent Snapshots recompute percentiles from the true merged distribution.
// other is left unmodified.
func (r *Recorder) Merge(other *Recorder) {
	om := other.collect()
	l := &Local{
		high:     r.high,
		sig:      r.sig,
		overall:  om.overall,
		byOp:     om.byOp,
		errByOp:  om.errByOp,
		errTotal: om.errTotal,
	}
	r.mu.Lock()
	r.locals = append(r.locals, l)
	r.mu.Unlock()
}
