package workload

import (
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// defaultZipfS is the Zipfian skew exponent when Params has no "zipf_s". It must
// be > 1 for math/rand/v2.NewZipf; ~1.1 gives a pronounced-but-not-degenerate hot set.
const defaultZipfS = 1.1

// Generator produces the next concrete Operation. NOT safe for concurrent use;
// one per goroutine.
type Generator interface {
	Next() domain.Operation
}

// Factory builds independent per-goroutine generators from a workload + seed.
// workerID/workerCount identify this load client among distributed workers so
// key ranges are partitioned (disjoint) UNLESS w.Params["shared_keys"]==true.
//
// Partitioning scheme: interleaved (strided) by worker. Worker w of N owns the
// global keys { k in [0,KeySpace) : k mod N == w }; a generator draws a local
// index i in [0, localSpace) and maps it to global key i*N + w. Interleaving
// (rather than contiguous blocks) keeps the hot keys of skewed distributions —
// which cluster at low indices — spread evenly across workers instead of piling
// onto worker 0. With shared_keys the stride collapses to 1 and every worker and
// shard draws from the full [0,KeySpace). Shards of one worker share that
// worker's partition but run independent RNG streams; per-key Seq is monotonic
// within a single generator (see domain.Operation).
type Factory struct {
	w          domain.Workload
	seed       int64
	workerID   int
	strideN    int64
	base       int64
	localSpace int64
	zipfS      float64
	cum        []int // cumulative operation weights
	total      int
}

// New validates the workload and computes this worker's key partition.
func New(w domain.Workload, seed int64, workerID, workerCount int) (*Factory, error) {
	if len(w.Operations) == 0 {
		return nil, fmt.Errorf("workload %q: no operations", w.Name)
	}
	if workerCount < 1 {
		return nil, fmt.Errorf("workerCount must be >= 1, got %d", workerCount)
	}
	if workerID < 0 || workerID >= workerCount {
		return nil, fmt.Errorf("workerID %d out of range [0,%d)", workerID, workerCount)
	}
	if w.KeySpace < 1 {
		return nil, fmt.Errorf("workload %q: key space must be >= 1, got %d", w.Name, w.KeySpace)
	}

	cum := make([]int, len(w.Operations))
	total := 0
	for i, op := range w.Operations {
		if op.Weight <= 0 {
			return nil, fmt.Errorf("workload %q: operation %q has non-positive weight %d", w.Name, op.Type, op.Weight)
		}
		total += op.Weight
		cum[i] = total
	}

	strideN, base, local := partition(w, workerID, workerCount)
	if local < 1 {
		return nil, fmt.Errorf("workload %q: worker %d/%d owns no keys of %d", w.Name, workerID, workerCount, w.KeySpace)
	}

	return &Factory{
		w: w, seed: seed, workerID: workerID,
		strideN: strideN, base: base, localSpace: local,
		zipfS: zipfExponent(w.Params), cum: cum, total: total,
	}, nil
}

// partition returns the stride, base offset, and local key count for a worker.
func partition(w domain.Workload, workerID, workerCount int) (stride, base, local int64) {
	if paramBool(w.Params, "shared_keys") {
		return 1, 0, w.KeySpace
	}
	stride = int64(workerCount)
	base = int64(workerID)
	if base >= w.KeySpace {
		return stride, base, 0
	}
	// ceil((KeySpace - base) / stride): count of base, base+stride, ... < KeySpace.
	local = (w.KeySpace - base + stride - 1) / stride
	return stride, base, local
}

// Generator returns the generator for goroutine index shard. Its RNG is seeded
// deterministically from (seed, workerID, shard), so the same inputs always
// reproduce the identical Operation stream.
func (f *Factory) Generator(shard int) Generator {
	// Two PCG words derived from seed+workerID+shard: distinct words avoid the
	// stream collisions a bare sum would cause (worker0/shard1 vs worker1/shard0).
	s1 := uint64(f.seed+int64(f.workerID)+int64(shard)) * 0x9E3779B97F4A7C15
	s2 := uint64(f.seed) ^ (uint64(f.workerID) << 32) ^ (uint64(shard) * 0xD1B54A32D192ED03)
	r := rand.New(rand.NewPCG(s1, s2))
	return &generator{
		f:     f,
		r:     r,
		keys:  newKeyGen(string(f.w.KeyDist), r, f.localSpace, f.zipfS),
		seqOf: make(map[int64]int64),
		vsize: f.w.ValueSize,
	}
}

// generator is a single goroutine's operation source. Lock-free by design.
type generator struct {
	f     *Factory
	r     *rand.Rand
	keys  keyGen
	seqOf map[int64]int64
	vsize int
}

// Next returns the next operation: a weighted op type, a distribution-drawn key
// mapped into this worker's partition, a per-key monotonic Seq, and a payload
// for writes (empty for reads).
func (g *generator) Next() domain.Operation {
	op := g.pickOp()
	key := g.keys.next()*g.f.strideN + g.f.base
	g.seqOf[key]++
	seq := g.seqOf[key]
	var val []byte
	if isWrite(op) {
		val = Value(key, g.vsize)
	}
	return domain.Operation{Type: op, Key: key, Value: val, Seq: seq}
}

// pickOp selects an operation type proportional to its weight.
func (g *generator) pickOp() domain.OpType {
	x := g.r.IntN(g.f.total)
	for i, c := range g.f.cum {
		if x < c {
			return g.f.w.Operations[i].Type
		}
	}
	return g.f.w.Operations[len(g.f.cum)-1].Type // unreachable; guards float drift
}

// readOps are the operation-type labels treated as reads (no payload). Any other
// label is a write and gets a Value. Matches domain examples: read vs
// insert/append/transfer/produce.
var readOps = map[string]struct{}{
	"read": {}, "get": {}, "scan": {}, "count": {}, "query": {},
}

func isWrite(t domain.OpType) bool {
	_, ok := readOps[strings.ToLower(string(t))]
	return !ok
}

func paramBool(p map[string]any, key string) bool {
	v, ok := p[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func zipfExponent(p map[string]any) float64 {
	v, ok := p["zipf_s"]
	if !ok {
		return defaultZipfS
	}
	if f, ok := v.(float64); ok && f > 1 {
		return f
	}
	return defaultZipfS
}
