// Package domain holds the ubiquitous language of proofload: the pure concepts
// (runs, workloads, operations, metrics, clusters, verification, faults) shared
// by every layer. It has no I/O and no dependencies on other proofload packages,
// so it can be imported freely without creating cycles.
package domain

import "time"

// RunMode distinguishes a throughput/latency benchmark from a correctness run.
// The two share engines and drivers but differ in workload, what is recorded,
// and what runs alongside (nemesis faults, history recording, verification).
type RunMode string

const (
	// ModePerformance drives load at a fixed rate or to saturation and records
	// HdrHistogram latencies. Verification, if any, is reconciliation-level.
	ModePerformance RunMode = "performance"
	// ModeCorrectness drives a checkable workload, records a full operation
	// history, may inject faults, and runs an isolation/linearizability checker.
	ModeCorrectness RunMode = "correctness"
)

// TestType is the archetype of a run — the aspect of the system it examines.
// It selects the rate profile and sensible guardrails; verification (--verify)
// and fault injection (--fault) compose on top of any type.
type TestType string

const (
	// TestBenchmark drives closed-loop to maximum sustainable throughput ("as
	// much as the client can push") and reports peak throughput + latency.
	TestBenchmark TestType = "benchmark"
	// TestLoad holds a constant open-loop arrival rate (a production-like load)
	// and reports the latency distribution at that rate.
	TestLoad TestType = "load"
	// TestStress ramps the arrival rate upward in steps until the target's knee
	// appears in the time series — it finds the breaking point.
	TestStress TestType = "stress"
	// TestACID exercises correctness (a verify model) under controlled load.
	TestACID TestType = "acid"
	// TestCombined runs load + faults + correctness together to observe behavior
	// under high load with failures.
	TestCombined TestType = "combined"
)

// Phase is a stage of a single run's load. Only the measure phase is recorded.
type Phase string

const (
	PhaseWarmup  Phase = "warmup"
	PhaseMeasure Phase = "measure"
)

// RunID uniquely identifies one run and names its results directory.
type RunID string

// RateMode selects open-loop (fixed arrival rate, coordinated-omission correct)
// versus closed-loop (drive to maximum sustainable throughput).
type RateMode string

const (
	RateFixed RateMode = "fixed"
	RateMax   RateMode = "max"
)

// RateSpec describes the load intensity for a run.
type RateSpec struct {
	Mode      RateMode `json:"mode"`
	OpsPerSec int      `json:"ops_per_sec,omitempty"` // used when Mode == RateFixed
}

// ClientInfo captures the machine generating load, for reproducibility.
type ClientInfo struct {
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUs     int    `json:"cpus"`
}

// Manifest is the full, self-describing reproducibility record persisted with
// every run. Two runs with equal manifests (modulo timestamps) are comparable.
type Manifest struct {
	RunID         RunID             `json:"run_id"`
	Target        string            `json:"target"`
	TargetVersion string            `json:"target_version,omitempty"`
	Workload      string            `json:"workload"`
	TestType      TestType          `json:"test_type,omitempty"`
	Mode          RunMode           `json:"mode"`
	Rate          RateSpec          `json:"rate"`
	Duration      time.Duration     `json:"duration"`
	Warmup        time.Duration     `json:"warmup"`
	Connections   int               `json:"connections"`
	Seed          int64             `json:"seed"`
	Trials        int               `json:"trials"`
	Consistency   string            `json:"consistency,omitempty"`
	Cluster       ClusterSpec       `json:"cluster"`
	Faults        []FaultSpec       `json:"faults,omitempty"`
	EngineVersion string            `json:"engine_version"`
	GitSHA        string            `json:"git_sha,omitempty"`
	Client        ClientInfo        `json:"client"`
	Labels        map[string]string `json:"labels,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
}

// Run is the in-memory handle for an executing or completed run.
type Run struct {
	ID        RunID
	Target    string
	Workload  string
	Mode      RunMode
	StartedAt time.Time
	Manifest  Manifest
}
