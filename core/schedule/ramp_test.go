package schedule

import (
	"sync"
	"testing"
	"time"
)

// TestRampTwoStepSpansAndGaps: a 1000 ops/s (1s) -> 2000 ops/s (1s) ramp issues
// 1000 times 1ms apart in [start,+1s), then 2000 times 0.5ms apart in [+1s,+2s).
func TestRampTwoStepSpansAndGaps(t *testing.T) {
	start := time.Now()
	s := NewRamp(start, []RateStep{
		{OpsPerSec: 1000, Duration: time.Second},
		{OpsPerSec: 2000, Duration: time.Second},
	})
	const total = 3000
	times := make([]time.Time, total)
	for i := range times {
		times[i] = s.Next()
	}
	if !times[0].Equal(start) {
		t.Fatalf("first intended = %v, want %v", times[0], start)
	}
	// Per index: assert the correct window [0,1s) or [1s,2s) and the correct gap.
	for i := 1; i < total; i++ {
		off, gap := times[i].Sub(start), times[i].Sub(times[i-1])
		wantGap, lo, hi := time.Millisecond, time.Duration(0), time.Second
		if i >= 1000 {
			wantGap, lo, hi = 500*time.Microsecond, time.Second, 2*time.Second
		}
		if off < lo || off >= hi {
			t.Fatalf("intended[%d] offset %v outside [%v,%v)", i, off, lo, hi)
		}
		if i != 1000 && gap != wantGap { // i==1000 is the boundary gap
			t.Fatalf("gap[%d] = %v, want %v", i, gap, wantGap)
		}
	}
	// Total intended span ~2s (exactly 1s + 1999*0.5ms).
	span := times[total-1].Sub(times[0])
	if want := time.Second + 1999*500*time.Microsecond; span != want {
		t.Fatalf("span = %v, want %v", span, want)
	}
}

// TestRampConcurrentContiguous: the atomic counter hands out a contiguous,
// unique, monotonic-by-index set of intended times under concurrency.
func TestRampConcurrentContiguous(t *testing.T) {
	start := time.Now()
	s := NewRamp(start, []RateStep{
		{OpsPerSec: 1000, Duration: time.Second},
		{OpsPerSec: 2000, Duration: time.Second},
	})
	const workers = 16
	const per = 200
	const total = workers * per // 3200: spills past the schedule into seg1's tail
	results := make([]time.Time, total)
	var wg sync.WaitGroup
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

	// The concurrent set must equal the deterministic tight-loop sequence:
	// unique, contiguous, gap-free (monotonic by claimed index).
	ref := NewRamp(start, []RateStep{
		{OpsPerSec: 1000, Duration: time.Second},
		{OpsPerSec: 2000, Duration: time.Second},
	})
	want := make(map[time.Duration]bool, total)
	for i := 0; i < total; i++ {
		want[ref.Next().Sub(start)] = true
	}
	if len(want) != total {
		t.Fatalf("reference has %d distinct times, want %d", len(want), total)
	}
	for _, got := range results {
		off := got.Sub(start)
		if !want[off] {
			t.Fatalf("concurrent result %v not in reference sequence", off)
		}
		delete(want, off)
	}
	if len(want) != 0 {
		t.Fatalf("%d reference times missing from concurrent results", len(want))
	}
}

// TestRampCoordinatedOmission asserts the sequence is independent of how slowly
// Next is called: a delayed caller must see identical timestamps to a tight loop.
func TestRampCoordinatedOmission(t *testing.T) {
	start := time.Now()
	mk := func() Scheduler {
		return NewRamp(start, []RateStep{
			{OpsPerSec: 500, Duration: time.Second},
			{OpsPerSec: 1500, Duration: time.Second},
		})
	}
	const n = 60
	tight := mk()
	tightTimes := make([]time.Time, n)
	for i := range tightTimes {
		tightTimes[i] = tight.Next()
	}
	delayed := mk()
	for i := 0; i < n; i++ {
		got := delayed.Next()
		time.Sleep(150 * time.Microsecond) // simulate a slow consumer
		if !tightTimes[i].Equal(got) {
			t.Fatalf("intended[%d] differs: tight=%v delayed=%v", i, tightTimes[i], got)
		}
	}
}

// TestRampAfterLastStep verifies that past the final step, Next keeps issuing at
// the last step's rate (here 2000 ops/s -> 0.5ms gaps) with a continuous offset.
func TestRampAfterLastStep(t *testing.T) {
	start := time.Now()
	s := NewRamp(start, []RateStep{
		{OpsPerSec: 1000, Duration: time.Second}, // 1000 ops, ends at offset 1s
		{OpsPerSec: 2000, Duration: time.Second}, // 2000 ops, ends at index 3000
	})
	const total = 5000 // 2000 indices beyond the schedule
	var prev time.Time
	for i := 0; i < total; i++ {
		got := s.Next()
		if i >= 3001 { // strictly inside the post-schedule tail
			if gap := got.Sub(prev); gap != 500*time.Microsecond {
				t.Fatalf("post-schedule gap[%d] = %v, want 500us", i, gap)
			}
		}
		prev = got
	}
	// Index 4999 -> 1s + (4999-1000)/2000 s.
	wantLast := start.Add(time.Second + offsetFor(3999, 2000))
	if !prev.Equal(wantLast) {
		t.Fatalf("last intended = %v, want %v", prev, wantLast)
	}
}

// TestLinearRamp: LinearRamp(1000..5000, 5 segments, 1s each) has five
// rising-rate segments and a ~5s total intended span.
func TestLinearRamp(t *testing.T) {
	start := time.Now()
	s := LinearRamp(start, 1000, 5000, 5, time.Second)

	// Rates 1000,2000,3000,4000,5000 => 1000+2000+3000+4000+5000 = 15000 ops.
	const total = 15000
	var first, last time.Duration = -1, 0
	var perSec [5]int // ops whose intended time falls in each 1-second window
	for i := 0; i < total; i++ {
		off := s.Next().Sub(start)
		if first < 0 {
			first = off
		}
		last = off
		if b := int(off / time.Second); b >= 0 && b < len(perSec) {
			perSec[b]++
		}
	}
	if first != 0 {
		t.Fatalf("first intended offset = %v, want 0", first)
	}
	// Five segments with strictly rising rates: 1000..5000 ops per window.
	if want := [5]int{1000, 2000, 3000, 4000, 5000}; perSec != want {
		t.Fatalf("ops per second-window = %v, want %v (5 rising segments)", perSec, want)
	}
	// Span: 4s (first four full seconds) + 4999/5000 s in the final segment ~5s.
	if wantSpan := 4*time.Second + offsetFor(4999, 5000); last != wantSpan {
		t.Fatalf("total span = %v, want %v (~5s)", last, wantSpan)
	}
}

// TestRampEmptyAndSkipped confirms empty/all-skipped ramps degrade to fire-now.
func TestRampEmptyAndSkipped(t *testing.T) {
	cases := map[string][]RateStep{
		"nil":     nil,
		"empty":   {},
		"nonpos":  {{OpsPerSec: 0, Duration: time.Second}, {OpsPerSec: -5, Duration: time.Second}},
		"zerodur": {{OpsPerSec: 1000, Duration: 0}},
	}
	for name, steps := range cases {
		s := NewRamp(time.Now().Add(-time.Hour), steps)
		for i := 0; i < 3; i++ {
			before := time.Now()
			got := s.Next()
			after := time.Now()
			if got.Before(before) || got.After(after) {
				t.Fatalf("%s: fire-now Next = %v, want within [%v,%v]", name, got, before, after)
			}
		}
	}
}
