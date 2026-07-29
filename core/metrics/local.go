package metrics

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

// Local is a per-goroutine latency sink. Each load goroutine calls
// Recorder.Local once and records into its own Local, so the hot path never
// contends on the parent Recorder's lock. A Local's small internal mutex is
// held only for the duration of a single Record and is uncontended in normal
// use (one recording goroutine); the aggregator briefly takes it while
// merging. A Local is safe for its owning goroutine to write while the
// aggregator reads it — do not share one Local across goroutines.
type Local struct {
	high int64
	sig  int

	mu       sync.Mutex
	overall  *hdrhistogram.Histogram
	byOp     map[domain.OpType]*hdrhistogram.Histogram
	errByOp  map[domain.OpType]int64
	errTotal int64
}

func newLocal(high int64, sig int) *Local {
	return &Local{
		high:    high,
		sig:     sig,
		overall: newHist(high, sig),
		byOp:    make(map[domain.OpType]*hdrhistogram.Histogram),
		errByOp: make(map[domain.OpType]int64),
	}
}

// Record adds one operation's wall-clock latency to this Local. The latency is
// clamped into the tracked range so tail values are never silently dropped.
// isErr marks the operation as failed; failed operations still contribute their
// latency (a timeout is a latency) and additionally bump the error counters.
func (l *Local) Record(op domain.OpType, latency time.Duration, isErr bool) {
	ns := int64(latency)
	if ns < 0 {
		ns = 0
	}
	if ns > l.high {
		ns = l.high
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.overall.RecordValue(ns)
	h := l.byOp[op]
	if h == nil {
		h = newHist(l.high, l.sig)
		l.byOp[op] = h
	}
	h.RecordValue(ns)

	if isErr {
		l.errByOp[op]++
		l.errTotal++
	}
}
