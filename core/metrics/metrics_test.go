package metrics

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

const opRead = domain.OpType("read")

// recordRange feeds latencies lo..hi milliseconds (inclusive, 1ms step) into a
// single Local of r under op.
func recordRange(r *Recorder, op domain.OpType, lo, hi int) {
	l := r.Local()
	for i := lo; i <= hi; i++ {
		l.Record(op, time.Duration(i)*time.Millisecond, false)
	}
}

func closeTo(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.3f, want %.3f (+/- %.3f)", name, got, want, tol)
	}
}

func TestPercentilesBasic(t *testing.T) {
	r := New(Options{})
	recordRange(r, opRead, 1, 100)

	res := r.Snapshot(domain.PhaseMeasure, time.Second)
	if res.Total != 100 {
		t.Fatalf("Total = %d, want 100", res.Total)
	}
	tests := []struct {
		name string
		got  float64
		want float64
	}{
		{"p50", res.Overall.P50, 50},
		{"p99", res.Overall.P99, 99},
		{"max", res.Overall.Max, 100},
	}
	for _, tc := range tests {
		closeTo(t, tc.name, tc.got, tc.want, 1.5)
	}
	if res.Throughput != 100 {
		t.Errorf("Throughput = %.2f, want 100", res.Throughput)
	}
}

func TestMergeEqualsSingle(t *testing.T) {
	a := New(Options{})
	recordRange(a, opRead, 1, 100)
	b := New(Options{})
	recordRange(b, opRead, 51, 150)

	combined := New(Options{})
	recordRange(combined, opRead, 1, 100)
	recordRange(combined, opRead, 51, 150)

	a.Merge(b)

	got := a.Snapshot(domain.PhaseMeasure, time.Second).Overall
	want := combined.Snapshot(domain.PhaseMeasure, time.Second).Overall
	if got.Count != want.Count {
		t.Fatalf("count: merged %d, single %d", got.Count, want.Count)
	}
	pcts := []struct {
		name      string
		got, want float64
	}{
		{"p50", got.P50, want.P50},
		{"p90", got.P90, want.P90},
		{"p99", got.P99, want.P99},
		{"p999", got.P999, want.P999},
		{"max", got.Max, want.Max},
	}
	for _, p := range pcts {
		if p.got != p.want {
			t.Errorf("%s: merged %.4f != single %.4f", p.name, p.got, p.want)
		}
	}
}

func TestTakeIntervalDeltasSumToTotal(t *testing.T) {
	r := New(Options{})
	opWrite := domain.OpType("write")

	l := r.Local()
	for i := 0; i < 30; i++ {
		l.Record(opRead, 5*time.Millisecond, false)
	}
	var total int64
	for _, s := range r.TakeInterval(time.Now(), time.Second) {
		total += s.Pct.Count
	}

	for i := 0; i < 20; i++ {
		l.Record(opRead, 7*time.Millisecond, false)
	}
	for i := 0; i < 10; i++ {
		l.Record(opWrite, 3*time.Millisecond, false)
	}
	for _, s := range r.TakeInterval(time.Now(), time.Second) {
		total += s.Pct.Count
	}

	if total != 60 {
		t.Fatalf("summed interval counts = %d, want 60", total)
	}
	final := r.Snapshot(domain.PhaseMeasure, time.Second)
	if final.Total != 60 {
		t.Fatalf("Snapshot total = %d, want 60", final.Total)
	}
}

func TestConcurrentRecord(t *testing.T) {
	r := New(Options{})
	const goroutines = 8
	const perG = 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := r.Local()
			for i := 0; i < perG; i++ {
				l.Record(opRead, time.Duration(i%50+1)*time.Millisecond, i%100 == 0)
			}
		}()
	}
	wg.Wait()

	res := r.Snapshot(domain.PhaseMeasure, time.Second)
	if want := int64(goroutines * perG); res.Total != want {
		t.Fatalf("Total = %d, want %d", res.Total, want)
	}
	if want := int64(goroutines * perG / 100); res.Errors != want {
		t.Fatalf("Errors = %d, want %d", res.Errors, want)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := New(Options{})
	l := r.Local()
	opWrite := domain.OpType("write")
	for i := 1; i <= 200; i++ {
		l.Record(opRead, time.Duration(i)*time.Millisecond, false)
	}
	for i := 1; i <= 50; i++ {
		l.Record(opWrite, time.Duration(i)*time.Millisecond, i%5 == 0)
	}

	blob, err := r.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := Decode(blob)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	orig := r.Snapshot(domain.PhaseMeasure, time.Second)
	got := dec.Snapshot(domain.PhaseMeasure, time.Second)
	if got.Total != orig.Total {
		t.Errorf("total: got %d, want %d", got.Total, orig.Total)
	}
	if got.Errors != orig.Errors {
		t.Errorf("errors: got %d, want %d", got.Errors, orig.Errors)
	}
	for op, wantP := range orig.ByOp {
		gotP, ok := got.ByOp[op]
		if !ok {
			t.Errorf("op %q missing after round trip", op)
			continue
		}
		if gotP.Count != wantP.Count {
			t.Errorf("op %q count: got %d, want %d", op, gotP.Count, wantP.Count)
		}
		if gotP.P99 != wantP.P99 {
			t.Errorf("op %q p99: got %.4f, want %.4f", op, gotP.P99, wantP.P99)
		}
	}
}
