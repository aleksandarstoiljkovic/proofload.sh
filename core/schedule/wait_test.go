package schedule

import (
	"context"
	"testing"
	"time"
)

// TestWaitUntilFuture waits for a near-future time and expects ~0 lateness.
func TestWaitUntilFuture(t *testing.T) {
	ctx := context.Background()
	target := time.Now().Add(20 * time.Millisecond)

	late, err := WaitUntil(ctx, target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if late < 0 || late > 10*time.Millisecond {
		t.Fatalf("late = %v, want ~0 for a future time", late)
	}
	if time.Now().Before(target) {
		t.Fatal("WaitUntil returned before the target time")
	}
}

// TestWaitUntilPast returns immediately and reports positive lateness.
func TestWaitUntilPast(t *testing.T) {
	ctx := context.Background()
	past := time.Now().Add(-50 * time.Millisecond)

	before := time.Now()
	late, err := WaitUntil(ctx, past)
	elapsed := time.Since(before)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if late <= 0 {
		t.Fatalf("late = %v, want > 0 for a past time", late)
	}
	if elapsed > 5*time.Millisecond {
		t.Fatalf("WaitUntil blocked %v for a past time; should be immediate", elapsed)
	}
}

// TestWaitUntilCancelled returns ctx.Err() promptly when the context is
// cancelled while waiting for a future time.
func TestWaitUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	target := time.Now().Add(time.Hour)

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	before := time.Now()
	late, err := WaitUntil(ctx, target)
	elapsed := time.Since(before)
	if err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if late != 0 {
		t.Fatalf("late = %v, want 0 on cancel", late)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("cancel not prompt: waited %v", elapsed)
	}
}
