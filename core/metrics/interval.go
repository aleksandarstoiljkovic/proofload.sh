package metrics

import (
	"sort"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

// TakeInterval returns the per-op latency delta since the previous call. It
// diffs the current cumulative histograms against the ones captured last time,
// so summing the returned Pct.Count across every call reconstructs the total
// count exactly. now stamps the snapshot; dt is the interval length used for
// throughput. Results are ordered by op type for determinism.
func (r *Recorder) TakeInterval(now time.Time, dt time.Duration) []domain.LatencySnapshot {
	m := r.collect()

	ops := make([]domain.OpType, 0, len(m.byOp))
	for op := range m.byOp {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })

	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]domain.LatencySnapshot, 0, len(ops))
	for _, op := range ops {
		cur := m.byOp[op]
		delta := deltaHist(cur, r.prevSnap[op])
		r.prevSnap[op] = cur.Export()

		errDelta := m.errByOp[op] - r.prevErr[op]
		r.prevErr[op] = m.errByOp[op]

		snap := domain.LatencySnapshot{
			T:      now,
			OpType: op,
			Errors: errDelta,
			Pct:    toPercentiles(delta),
		}
		if dt > 0 {
			snap.Throughput = float64(delta.TotalCount()) / dt.Seconds()
		}
		out = append(out, snap)
	}
	return out
}

// deltaHist returns cur minus the previously captured cumulative snapshot,
// bin by bin. A nil prev means the whole of cur is new. cur and prev share
// identical histogram geometry, so the counts arrays align index-for-index.
func deltaHist(cur *hdrhistogram.Histogram, prev *hdrhistogram.Snapshot) *hdrhistogram.Histogram {
	cs := cur.Export()
	if prev == nil {
		return hdrhistogram.Import(cs)
	}
	counts := make([]int64, len(cs.Counts))
	for i := range cs.Counts {
		d := cs.Counts[i]
		if i < len(prev.Counts) {
			d -= prev.Counts[i]
		}
		if d < 0 {
			d = 0
		}
		counts[i] = d
	}
	return hdrhistogram.Import(&hdrhistogram.Snapshot{
		LowestTrackableValue:  cs.LowestTrackableValue,
		HighestTrackableValue: cs.HighestTrackableValue,
		SignificantFigures:    cs.SignificantFigures,
		Counts:                counts,
	})
}
