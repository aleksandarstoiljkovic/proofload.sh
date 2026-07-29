package schedule

import (
	"context"
	"time"
)

// WaitUntil blocks until the intended time t, then reports how late the wakeup
// was relative to t (0 when on time or early). Lateness is the caller's signal
// that the load generator itself is saturated: a persistently positive late
// value means the client cannot keep up with the schedule, so recorded latencies
// would understate the true coordinated-omission-corrected picture.
//
// The wait uses a monotonic timer via time.Until, so it is immune to wall-clock
// adjustments. If t is already in the past, WaitUntil returns immediately with
// the elapsed lateness. It returns ctx.Err() promptly if ctx is cancelled while
// waiting.
func WaitUntil(ctx context.Context, t time.Time) (late time.Duration, err error) {
	d := time.Until(t)
	if d <= 0 {
		// Already due: we are late by however far past t we are.
		return -d, nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
		late = time.Since(t)
		if late < 0 {
			late = 0
		}
		return late, nil
	}
}
