package cli

import (
	"fmt"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/runner"
	"github.com/proofload/proofload/core/schedule"
)

// resolveTestType maps the --test flag to a TestType and validates the flag
// combination. An empty flag defaults to load when a rate is set, else benchmark.
func resolveTestType(f *runFlags) (domain.TestType, error) {
	switch f.test {
	case "":
		if f.rate > 0 {
			return domain.TestLoad, nil
		}
		return domain.TestBenchmark, nil
	case "benchmark":
		return domain.TestBenchmark, nil
	case "load":
		if f.rate <= 0 {
			return "", fmt.Errorf("--test load requires --rate (the constant arrival rate)")
		}
		return domain.TestLoad, nil
	case "stress":
		if f.rate <= 0 {
			return "", fmt.Errorf("--test stress requires --rate (the ramp ceiling)")
		}
		return domain.TestStress, nil
	case "acid":
		return domain.TestACID, nil
	case "combined":
		return domain.TestCombined, nil
	default:
		return "", fmt.Errorf("unknown --test %q (want benchmark|load|stress|acid|combined)", f.test)
	}
}

// buildScheduler selects the arrival schedule for a test type:
//   - benchmark: closed-loop, saturate the target (rate ignored).
//   - stress:    ramp the rate up in 5 steps to the --rate ceiling, so the
//     per-second time series reveals the knee.
//   - load/acid/combined (and anything else): constant open-loop at --rate,
//     degrading to closed-loop when no fixed rate is set.
func buildScheduler(start time.Time, tt domain.TestType, rate domain.RateSpec, duration time.Duration) runner.Scheduler {
	switch tt {
	case domain.TestBenchmark:
		return schedule.NewClosedLoop(start)
	case domain.TestStress:
		top := rate.OpsPerSec
		from := top / 5
		if from < 1 {
			from = 1
		}
		step := duration / 5
		if step <= 0 {
			step = time.Second
		}
		return schedule.LinearRamp(start, from, top, 5, step)
	default:
		return schedule.NewOpenLoop(start, rate)
	}
}
