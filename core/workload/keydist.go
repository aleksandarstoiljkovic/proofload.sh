package workload

import "math/rand/v2"

// keyGen draws the next LOCAL key index in [0, span). The Factory maps that
// local index onto the worker's disjoint global partition. keyGen is not safe
// for concurrent use; each Generator owns one.
type keyGen interface {
	next() int64
}

// newKeyGen builds the key generator for dist over a worker-local space of the
// given span, driven by r. zipfS is the Zipfian skew exponent (>1; larger = more
// skewed). An unknown dist falls back to uniform.
func newKeyGen(dist string, r *rand.Rand, span int64, zipfS float64) keyGen {
	if span <= 1 {
		return constKeys{}
	}
	switch dist {
	case "zipfian":
		return &zipfKeys{z: rand.NewZipf(r, zipfS, 1, uint64(span-1))}
	case "latest":
		return &latestKeys{z: rand.NewZipf(r, zipfS, 1, uint64(span-1)), span: span, head: 1}
	case "sequential":
		return &sequentialKeys{span: span}
	default: // uniform and any unrecognised distribution
		return &uniformKeys{r: r, span: span}
	}
}

// constKeys always returns 0; used when the local space holds a single key.
type constKeys struct{}

func (constKeys) next() int64 { return 0 }

// uniformKeys draws every key with equal probability.
type uniformKeys struct {
	r    *rand.Rand
	span int64
}

func (u *uniformKeys) next() int64 { return u.r.Int64N(u.span) }

// sequentialKeys scans keys in order and wraps at span, staying inside the
// local space (and therefore inside KeySpace after mapping).
type sequentialKeys struct {
	span int64
	cur  int64
}

func (s *sequentialKeys) next() int64 {
	k := s.cur % s.span
	s.cur++
	return k
}

// zipfKeys draws a skewed distribution where low indices are hot.
type zipfKeys struct{ z *rand.Zipf }

func (z *zipfKeys) next() int64 { return int64(z.z.Uint64()) }

// latestKeys biases toward recently "inserted" keys: an insert head grows on
// every call up to span, and a Zipfian offset from the head makes the newest
// keys the hottest. Models an insert-heavy workload's recency skew.
type latestKeys struct {
	z    *rand.Zipf
	span int64
	head int64
}

func (l *latestKeys) next() int64 {
	if l.head < l.span {
		l.head++
	}
	off := int64(l.z.Uint64())
	if off >= l.head {
		off = l.head - 1
	}
	return l.head - 1 - off
}
