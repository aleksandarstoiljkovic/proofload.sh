package workload

import (
	"bytes"
	"testing"

	"github.com/proofload/proofload/core/domain"
)

func baseWorkload() domain.Workload {
	return domain.Workload{
		Name:      "test",
		Mode:      domain.ModePerformance,
		KeySpace:  10000,
		ValueSize: 100,
		KeyDist:   domain.DistUniform,
		Operations: []domain.OpSpec{
			{Type: "read", Weight: 80},
			{Type: "insert", Weight: 20},
		},
	}
}

func mustFactory(t *testing.T, w domain.Workload, seed int64, id, n int) *Factory {
	t.Helper()
	f, err := New(w, seed, id, n)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*domain.Workload)
		id, n   int
		wantErr bool
	}{
		{"ok", func(*domain.Workload) {}, 0, 1, false},
		{"no ops", func(w *domain.Workload) { w.Operations = nil }, 0, 1, true},
		{"zero weight", func(w *domain.Workload) { w.Operations[0].Weight = 0 }, 0, 1, true},
		{"neg weight", func(w *domain.Workload) { w.Operations[0].Weight = -1 }, 0, 1, true},
		{"zero keyspace", func(w *domain.Workload) { w.KeySpace = 0 }, 0, 1, true},
		{"bad workerCount", func(*domain.Workload) {}, 0, 0, true},
		{"workerID out of range", func(*domain.Workload) {}, 3, 3, true},
		{"worker owns no keys", func(w *domain.Workload) { w.KeySpace = 2 }, 5, 8, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := baseWorkload()
			tc.mutate(&w)
			_, err := New(w, 1, tc.id, tc.n)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// (1) op-mix ratios stay within tolerance over 100k ops.
func TestOpMixRatios(t *testing.T) {
	w := baseWorkload()
	g := mustFactory(t, w, 42, 0, 1).Generator(0)
	const total = 100_000
	counts := map[domain.OpType]int{}
	for range total {
		counts[g.Next().Type]++
	}
	for _, spec := range w.Operations {
		want := float64(spec.Weight) / 100.0
		got := float64(counts[spec.Type]) / total
		if diff := got - want; diff < -0.02 || diff > 0.02 {
			t.Errorf("op %q ratio=%.4f want≈%.4f", spec.Type, got, want)
		}
	}
}

// (2) same (workload, seed, workerID, shard) -> identical stream.
func TestDeterminism(t *testing.T) {
	w := baseWorkload()
	w.KeyDist = domain.DistZipfian
	newStream := func() []domain.Operation {
		g := mustFactory(t, w, 7, 1, 4).Generator(2)
		ops := make([]domain.Operation, 5000)
		for i := range ops {
			ops[i] = g.Next()
		}
		return ops
	}
	a, b := newStream(), newStream()
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Key != b[i].Key || a[i].Seq != b[i].Seq {
			t.Fatalf("op %d differs: %+v vs %+v", i, a[i], b[i])
		}
		if !bytes.Equal(a[i].Value, b[i].Value) {
			t.Fatalf("op %d value differs", i)
		}
	}
	// A different shard must diverge.
	c := mustFactory(t, w, 7, 1, 4).Generator(3)
	diverged := false
	for i := range a {
		if a[i].Key != c.Next().Key {
			diverged = true
			break
		}
	}
	if !diverged {
		t.Error("different shard produced identical key stream")
	}
}

// (3) zipfian concentrates load on a few keys vs uniform.
func TestZipfianSkew(t *testing.T) {
	const n = 100_000
	topShare := func(dist domain.KeyDistribution) float64 {
		w := baseWorkload()
		w.KeyDist = dist
		g := mustFactory(t, w, 3, 0, 1).Generator(0)
		counts := map[int64]int{}
		for range n {
			counts[g.Next().Key]++
		}
		// share of hits landing on the 10 hottest keys.
		var top [10]int
		for _, c := range counts {
			min, idx := top[0], 0
			for i, v := range top {
				if v < min {
					min, idx = v, i
				}
			}
			if c > min {
				top[idx] = c
			}
		}
		sum := 0
		for _, v := range top {
			sum += v
		}
		return float64(sum) / n
	}
	uni, zipf := topShare(domain.DistUniform), topShare(domain.DistZipfian)
	if zipf <= uni*5 {
		t.Errorf("zipf top-10 share=%.4f not markedly above uniform=%.4f", zipf, uni)
	}
	if zipf < 0.05 {
		t.Errorf("zipf skew too weak: top-10 share=%.4f", zipf)
	}
}

// (4) partitioned mode: two shards and two workerIDs produce disjoint key sets.
func TestPartitionDisjoint(t *testing.T) {
	w := baseWorkload()
	w.KeySpace = 1000
	keySet := func(id, n, shard int) map[int64]bool {
		g := mustFactory(t, w, 9, id, n).Generator(shard)
		set := map[int64]bool{}
		for range 20_000 {
			set[g.Next().Key] = true
		}
		return set
	}
	// Two distinct workers: every worker's keys satisfy key%N==workerID, so
	// their key sets are disjoint, tested here across different shards too.
	w0 := keySet(0, 2, 0)
	w1 := keySet(1, 2, 1)
	for k := range w0 {
		if k%2 != 0 {
			t.Fatalf("worker 0 emitted key %d outside its partition", k)
		}
		if w1[k] {
			t.Fatalf("key %d in both worker partitions", k)
		}
	}
	for k := range w1 {
		if k%2 != 1 {
			t.Fatalf("worker 1 emitted key %d outside its partition", k)
		}
	}
	// shared_keys disables partitioning: both workers reach even keys.
	w.Params = map[string]any{"shared_keys": true}
	s0 := keySet(0, 2, 0)
	sawEven := false
	for k := range keySet(1, 2, 0) {
		if k%2 == 0 {
			sawEven = true
			break
		}
	}
	if !sawEven {
		t.Error("shared_keys worker 1 never touched an even key")
	}
	if len(s0) == 0 {
		t.Error("shared_keys worker 0 produced no keys")
	}
}

// (5) Value is deterministic and exactly length n.
func TestValue(t *testing.T) {
	for _, size := range []int{0, 1, 7, 8, 9, 100, 4096} {
		a := Value(42, size)
		b := Value(42, size)
		if len(a) != max(size, 0) {
			t.Errorf("size %d: len=%d", size, len(a))
		}
		if !bytes.Equal(a, b) {
			t.Errorf("size %d: not deterministic", size)
		}
	}
	if bytes.Equal(Value(1, 64), Value(2, 64)) {
		t.Error("distinct keys produced identical values")
	}
	if Value(1, 0) != nil || Value(1, -5) != nil {
		t.Error("non-positive size should yield nil")
	}
}

// (6) sequential wraps within KeySpace and covers the partition in order.
func TestSequentialWrap(t *testing.T) {
	w := baseWorkload()
	w.KeySpace = 50
	w.KeyDist = domain.DistSequential
	w.Operations = []domain.OpSpec{{Type: "insert", Weight: 1}}
	f := mustFactory(t, w, 1, 0, 1)
	g := f.Generator(0)
	first := make([]int64, w.KeySpace)
	for i := range first {
		op := g.Next()
		if op.Key < 0 || op.Key >= w.KeySpace {
			t.Fatalf("key %d outside KeySpace %d", op.Key, w.KeySpace)
		}
		first[i] = op.Key
	}
	// After KeySpace ops it must wrap to the same sequence.
	for i := range first {
		if got := g.Next().Key; got != first[i] {
			t.Fatalf("wrap mismatch at %d: got %d want %d", i, got, first[i])
		}
	}
}

// Seq is monotonic per key within a generator.
func TestSeqMonotonicPerKey(t *testing.T) {
	w := baseWorkload()
	w.KeySpace = 20
	g := mustFactory(t, w, 5, 0, 1).Generator(0)
	last := map[int64]int64{}
	for range 5000 {
		op := g.Next()
		if op.Seq != last[op.Key]+1 {
			t.Fatalf("key %d: seq %d, expected %d", op.Key, op.Seq, last[op.Key]+1)
		}
		last[op.Key] = op.Seq
	}
}

// Reads carry no payload; writes carry Value(key, ValueSize).
func TestValuePresence(t *testing.T) {
	g := mustFactory(t, baseWorkload(), 1, 0, 1).Generator(0)
	for range 2000 {
		op := g.Next()
		switch op.Type {
		case "read":
			if op.Value != nil {
				t.Fatalf("read carried a value")
			}
		case "insert":
			if !bytes.Equal(op.Value, Value(op.Key, 100)) {
				t.Fatalf("insert value mismatch for key %d", op.Key)
			}
		}
	}
}
