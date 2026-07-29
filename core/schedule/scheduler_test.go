package schedule

import (
	"sync"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

func fixed(ops int) domain.RateSpec {
	return domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: ops}
}

// TestOpenLoopSpansAndGaps checks that at 1000 ops/s, 1000 intended times span
// ~1s and that consecutive gaps are ~1ms.
func TestOpenLoopSpansAndGaps(t *testing.T) {
	start := time.Now()
	s := NewOpenLoop(start, fixed(1000))

	const n = 1000
	times := make([]time.Time, n)
	for i := range times {
		times[i] = s.Next()
	}

	// First intended time is exactly start; last is start + 999/1000 s.
	if !times[0].Equal(start) {
		t.Fatalf("first intended = %v, want %v", times[0], start)
	}
	span := times[n-1].Sub(times[0])
	wantSpan := time.Duration(n-1) * time.Millisecond
	if span != wantSpan {
		t.Fatalf("span = %v, want %v", span, wantSpan)
	}

	for i := 1; i < n; i++ {
		gap := times[i].Sub(times[i-1])
		if gap != time.Millisecond {
			t.Fatalf("gap[%d] = %v, want 1ms", i, gap)
		}
	}
}

// TestOpenLoopCoordinatedOmission asserts that the Next sequence is a pure
// function of start+rate: injecting artificial delays between calls must not
// change the intended times versus a tight loop.
func TestOpenLoopCoordinatedOmission(t *testing.T) {
	start := time.Now()
	const n = 50

	tight := NewOpenLoop(start, fixed(500))
	tightTimes := make([]time.Time, n)
	for i := range tightTimes {
		tightTimes[i] = tight.Next()
	}

	delayed := NewOpenLoop(start, fixed(500))
	delayedTimes := make([]time.Time, n)
	for i := range delayedTimes {
		delayedTimes[i] = delayed.Next()
		time.Sleep(200 * time.Microsecond) // simulate a slow caller
	}

	for i := 0; i < n; i++ {
		if !tightTimes[i].Equal(delayedTimes[i]) {
			t.Fatalf("intended[%d] differs: tight=%v delayed=%v",
				i, tightTimes[i], delayedTimes[i])
		}
	}
}

// TestOpenLoopConcurrentContiguous verifies the atomic counter hands out a
// contiguous set of intended times with no duplicates or gaps under concurrency.
func TestOpenLoopConcurrentContiguous(t *testing.T) {
	start := time.Now()
	const ops = 1000
	const total = 10000
	s := NewOpenLoop(start, fixed(ops))

	var wg sync.WaitGroup
	results := make([]time.Time, total)
	const workers = 16
	per := total / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				results[base+i] = s.Next()
			}
		}(w * per)
	}
	wg.Wait()

	// Map each intended time back to its index n via offsetFor and confirm the
	// set is exactly {0, ..., total-1}.
	seen := make([]bool, total)
	for _, got := range results {
		off := got.Sub(start)
		// n = round(off / (1s/ops)); reconstruct exactly using offsetFor.
		n := int(int64(off) * ops / int64(time.Second))
		if n < 0 || n >= total {
			t.Fatalf("intended time %v maps to out-of-range n=%d", got, n)
		}
		if !got.Equal(start.Add(offsetFor(int64(n), ops))) {
			t.Fatalf("intended time %v does not match offsetFor(%d)", got, n)
		}
		if seen[n] {
			t.Fatalf("duplicate intended index n=%d", n)
		}
		seen[n] = true
	}
	for n, ok := range seen {
		if !ok {
			t.Fatalf("missing intended index n=%d (gap)", n)
		}
	}
}

// TestClosedLoopFireNow verifies closed-loop Next fires immediately (intended
// time is "now", never later) so WaitUntil does not block and latency is
// measured as service time, not elapsed-since-start.
func TestClosedLoopFireNow(t *testing.T) {
	s := NewClosedLoop(time.Now().Add(-time.Hour))
	for i := 0; i < 5; i++ {
		before := time.Now()
		got := s.Next()
		after := time.Now()
		if got.Before(before) || got.After(after) {
			t.Fatalf("closed-loop Next = %v, want within [%v, %v]", got, before, after)
		}
	}
}

// TestOpenLoopNonFixedIsClosedLoop checks that a non-fixed rate degrades to
// closed-loop behavior: each Next fires at ~now, never in the future.
func TestOpenLoopNonFixedIsClosedLoop(t *testing.T) {
	s := NewOpenLoop(time.Now(), domain.RateSpec{Mode: domain.RateMax})
	for i := 0; i < 3; i++ {
		before := time.Now()
		got := s.Next()
		if got.Before(before.Add(-time.Millisecond)) || got.After(time.Now()) {
			t.Fatalf("non-fixed open loop should fire ~now, got %v", got)
		}
	}
}

// TestPoissonDeterministic checks that a fixed seed yields an identical sequence
// and that arrivals are non-decreasing.
func TestPoissonDeterministic(t *testing.T) {
	start := time.Now()
	const n = 200
	seq := func() []time.Time {
		s := NewPoisson(start, fixed(1000), 42)
		out := make([]time.Time, n)
		for i := range out {
			out[i] = s.Next()
		}
		return out
	}
	a, b := seq(), seq()
	for i := 0; i < n; i++ {
		if !a[i].Equal(b[i]) {
			t.Fatalf("seed 42 not deterministic at %d: %v vs %v", i, a[i], b[i])
		}
		if i > 0 && a[i].Before(a[i-1]) {
			t.Fatalf("arrival %d went backwards: %v < %v", i, a[i], a[i-1])
		}
	}

	// A different seed should (almost surely) differ.
	other := NewPoisson(start, fixed(1000), 7)
	diff := false
	for i := 0; i < n; i++ {
		if !other.Next().Equal(a[i]) {
			diff = true
		}
	}
	if !diff {
		t.Fatal("different seed produced identical sequence")
	}
}

// TestPoissonMeanRate sanity-checks that the mean arrival rate is close to λ.
func TestPoissonMeanRate(t *testing.T) {
	start := time.Now()
	const ops = 1000
	const n = 20000
	s := NewPoisson(start, fixed(ops), 123)
	var last time.Time
	for i := 0; i < n; i++ {
		last = s.Next()
	}
	elapsed := last.Sub(start).Seconds()
	got := float64(n) / elapsed
	if got < ops*0.9 || got > ops*1.1 {
		t.Fatalf("mean rate = %.1f ops/s, want ~%d", got, ops)
	}
}
