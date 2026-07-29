package schedule

import (
	"math/rand"
	"sync"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// poisson is an open-loop scheduler whose inter-arrival gaps are exponentially
// distributed, producing a Poisson arrival process with mean rate λ. Successive
// gaps are drawn from a seeded PRNG under a mutex, so for a given seed the
// sequence of intended times is fully deterministic regardless of how many
// goroutines call Next or in what order.
type poisson struct {
	start  time.Time
	lambda float64 // arrivals per second, > 0

	mu  sync.Mutex
	rng *rand.Rand
	cum time.Duration // cumulative offset from start
}

// NewPoisson returns a Poisson (exponential inter-arrival) open-loop scheduler
// seeded by seed. For a fixed seed the emitted sequence is deterministic. If
// rate.Mode is not RateFixed (or the rate is non-positive) a closed-loop
// scheduler is returned instead.
func NewPoisson(start time.Time, rate domain.RateSpec, seed int64) Scheduler {
	if rate.Mode != domain.RateFixed || rate.OpsPerSec <= 0 {
		return NewClosedLoop(start)
	}
	return &poisson{
		start:  start,
		lambda: float64(rate.OpsPerSec),
		rng:    rand.New(rand.NewSource(seed)),
	}
}

// Next advances the arrival process by one exponentially distributed gap and
// returns the resulting intended time. The mutex serializes draws so the
// cumulative sequence is deterministic under concurrency.
func (p *poisson) Next() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	// ExpFloat64 has mean 1; dividing by lambda yields mean 1/λ seconds.
	gap := p.rng.ExpFloat64() / p.lambda
	p.cum += time.Duration(gap * float64(time.Second))
	return p.start.Add(p.cum)
}
