package schedule

import (
	"sort"
	"sync/atomic"
	"time"
)

// RateStep is one constant-rate segment of a ramp: the aggregate arrival rate is
// OpsPerSec for Duration of intended (scheduled) time.
type RateStep struct {
	OpsPerSec int
	Duration  time.Duration
}

// rampSeg is a precomputed segment. startCount is the 0-based index of the first
// op issued in this segment, and startOffset is that op's intended offset from
// the ramp's start. rate is the segment's ops-per-second (> 0). Precomputing both
// cumulative boundaries makes Next a pure function of the claimed index n, with
// no wall-clock reads, so intended times are coordinated-omission correct.
type rampSeg struct {
	startCount  int64
	startOffset time.Duration
	rate        int64
}

// ramp is a stepped open-loop scheduler. It walks its segments in order, issuing
// intended times at each segment's rate for that segment's duration of scheduled
// time, then continues at the final segment's rate after the last step. Next
// claims a unique 0-based index via an atomic counter, so it is safe for many
// goroutines to share one ramp and receive a contiguous, unique set of times.
type ramp struct {
	start time.Time
	segs  []rampSeg // ordered by startCount ascending; segs[0].startCount == 0
	n     atomic.Int64
}

// NewRamp returns a Scheduler that walks steps in order: during step i it issues
// intended times at steps[i].OpsPerSec for steps[i].Duration of intended time,
// then advances to the next step. After the last step it keeps issuing at the
// last step's rate. Next is concurrency-safe and coordinated-omission correct:
// the intended time of the nth claimed op is a pure function of start and the
// step schedule.
//
// A step with a non-positive rate or duration is skipped entirely (it issues no
// ops and advances neither the count nor the intended-time offset). If no usable
// steps remain (nil, empty, or all skipped) the ramp degrades to a closed-loop,
// fire-now scheduler, matching NewOpenLoop's behavior for a non-fixed rate.
func NewRamp(start time.Time, steps []RateStep) Scheduler {
	var (
		segs   []rampSeg
		count  int64
		offset time.Duration
	)
	for _, s := range steps {
		if s.OpsPerSec <= 0 || s.Duration <= 0 {
			continue
		}
		rate := int64(s.OpsPerSec)
		segs = append(segs, rampSeg{
			startCount:  count,
			startOffset: offset,
			rate:        rate,
		})
		count += opsInStep(rate, s.Duration)
		offset += s.Duration
	}
	if len(segs) == 0 {
		return NewClosedLoop(start)
	}
	return &ramp{start: start, segs: segs}
}

// LinearRamp builds a ramp of nsteps constant-rate segments, each of length each,
// with rates evenly interpolated from `from` to `to` ops/sec (inclusive at both
// ends). With nsteps <= 1 it yields a single segment at rate `to`. It is a thin
// convenience over NewRamp and shares all of its semantics.
func LinearRamp(start time.Time, from, to, nsteps int, each time.Duration) Scheduler {
	if nsteps <= 1 {
		return NewRamp(start, []RateStep{{OpsPerSec: to, Duration: each}})
	}
	steps := make([]RateStep, nsteps)
	for i := range steps {
		// Integer interpolation: rate == from + (to-from)*i/(nsteps-1), which is
		// exact at i == 0 (from) and i == nsteps-1 (to).
		rate := from + (to-from)*i/(nsteps-1)
		steps[i] = RateStep{OpsPerSec: rate, Duration: each}
	}
	return NewRamp(start, steps)
}

// Next returns the intended start time of the next operation for a freshly
// claimed, unique index n.
func (r *ramp) Next() time.Time {
	n := r.n.Add(1) - 1 // atomically claim a unique 0-based index
	return r.start.Add(r.offsetForIndex(n))
}

// offsetForIndex maps a 0-based op index to its intended offset from start. It
// locates the segment containing n (the last segment whose startCount <= n; the
// final segment has no upper bound, so indices past the schedule continue at its
// rate) and adds the within-segment offset.
func (r *ramp) offsetForIndex(n int64) time.Duration {
	i := sort.Search(len(r.segs), func(k int) bool {
		return r.segs[k].startCount > n
	}) - 1
	seg := r.segs[i]
	return seg.startOffset + offsetFor(n-seg.startCount, seg.rate)
}

// opsInStep returns floor(rate * dur) ops, where dur is measured in seconds. The
// duration is split into whole seconds plus a nanosecond remainder so the
// intermediate product cannot overflow int64 for any realistic segment.
func opsInStep(rate int64, dur time.Duration) int64 {
	if rate <= 0 || dur <= 0 {
		return 0
	}
	secs := int64(dur / time.Second)
	rem := int64(dur % time.Second)
	return rate*secs + rate*rem/int64(time.Second)
}
