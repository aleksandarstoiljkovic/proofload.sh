// Package schedule provides coordinated-omission-correct rate schedulers and a
// pacer for proofload's load generators.
//
// The central idea is open-loop scheduling: the intended start time of the Nth
// operation is fixed at start + N/λ, independent of how fast or slow the target
// responds. Because intended times never shift, a stalled target cannot hide
// latency — the runner measures time.Since(intendedStart), so backlog shows up
// as growing latency rather than as an artificially throttled request rate.
//
// This package depends only on the standard library and core/domain.
package schedule

import (
	"sync/atomic"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// Scheduler hands out intended start times for successive operations. A single
// Scheduler represents one aggregate rate shared by every caller, so Next is
// safe to call concurrently from many goroutines: each call receives a distinct,
// monotonically increasing intended time.
type Scheduler interface {
	// Next returns the intended start time of the next operation.
	Next() time.Time
}

// openLoop is a constant-arrival-rate scheduler. The intended time of the nth
// operation (0-based) is start + n/λ, computed purely from n so it never depends
// on wall-clock time or on how long callers take between calls.
type openLoop struct {
	start time.Time
	ops   int64        // aggregate ops per second, > 0
	n     atomic.Int64 // count of intended times handed out
}

// NewOpenLoop returns an open-loop scheduler for a constant aggregate arrival
// rate of rate.OpsPerSec across all callers, where intended[n] = start + n/λ.
// If rate.Mode is not RateFixed (or the rate is non-positive) the load is
// closed-loop, so a closed-loop scheduler is returned instead.
func NewOpenLoop(start time.Time, rate domain.RateSpec) Scheduler {
	if rate.Mode != domain.RateFixed || rate.OpsPerSec <= 0 {
		return NewClosedLoop(start)
	}
	return &openLoop{start: start, ops: int64(rate.OpsPerSec)}
}

// Next returns start + n/λ for a freshly claimed, unique index n.
func (o *openLoop) Next() time.Time {
	n := o.n.Add(1) - 1 // atomically claim a unique 0-based index
	return o.start.Add(offsetFor(n, o.ops))
}

// closedLoop drives the target to maximum sustainable throughput: there is no
// arrival schedule, so each operation's intended start is the instant it is
// dispatched. Next therefore returns the current time, which makes the runner's
// time.Since(intendedStart) measure true per-operation service latency (rather
// than time elapsed since the run began). It never blocks and holds no state, so
// it is trivially concurrency-safe.
type closedLoop struct{}

// NewClosedLoop returns a closed-loop (max-throughput) scheduler. The start
// argument is accepted for signature symmetry with NewOpenLoop but is unused,
// since closed-loop operations are intended at their dispatch instant.
func NewClosedLoop(_ time.Time) Scheduler {
	return closedLoop{}
}

// Next returns the current time, so the caller fires immediately and latency is
// measured from now.
func (closedLoop) Next() time.Time {
	return time.Now()
}

// offsetFor returns n/λ as a Duration where λ == ops per second. It splits the
// division into whole seconds plus a remainder so the intermediate product
// cannot overflow int64 for any realistic operation count.
func offsetFor(n, ops int64) time.Duration {
	if ops <= 0 {
		return 0
	}
	secs := n / ops
	rem := n % ops
	return time.Duration(secs)*time.Second +
		time.Duration(rem)*time.Second/time.Duration(ops)
}
