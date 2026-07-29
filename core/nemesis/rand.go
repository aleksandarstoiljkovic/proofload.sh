package nemesis

// prng is a tiny deterministic pseudo-random generator (SplitMix64). It picks
// fault target nodes reproducibly: it deliberately avoids math/rand, crypto/rand
// and any time-based seeding, so a given Seed always yields the same selection
// sequence and thus the same fault timeline across runs.
type prng struct{ state uint64 }

func newPRNG(seed int64) *prng { return &prng{state: uint64(seed)} }

// next returns the next 64-bit value in the SplitMix64 sequence.
func (p *prng) next() uint64 {
	p.state += 0x9E3779B97F4A7C15
	z := p.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// intn returns a value in [0, n). It returns 0 for non-positive n.
func (p *prng) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(p.next() % uint64(n))
}
