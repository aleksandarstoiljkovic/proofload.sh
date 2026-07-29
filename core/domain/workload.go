package domain

// OpType is a target-defined operation label (e.g. "read", "insert", "transfer",
// "append", "produce"). Drivers interpret it; the runner only uses it to bucket
// per-operation latency histograms.
type OpType string

// KeyDistribution controls how the workload generator picks keys, which strongly
// affects cache/hotspot behavior and thus measured latency.
type KeyDistribution string

const (
	DistUniform    KeyDistribution = "uniform"
	DistZipfian    KeyDistribution = "zipfian"
	DistLatest     KeyDistribution = "latest"
	DistSequential KeyDistribution = "sequential"
)

// OpSpec is one entry in a workload's weighted operation mix.
type OpSpec struct {
	Type   OpType `json:"type"`
	Weight int    `json:"weight"`
}

// Workload is the declarative description of what load to generate. It is parsed
// from targets/<db>/workloads/<name>.yaml and specialized per driver.
type Workload struct {
	Name        string          `json:"name"`
	Mode        RunMode         `json:"mode"`
	Operations  []OpSpec        `json:"operations"`
	KeySpace    int64           `json:"key_space"`
	KeyDist     KeyDistribution `json:"key_dist"`
	ValueSize   int             `json:"value_size"`
	VerifyModel VerifyModel     `json:"verify_model,omitempty"`
	Params      map[string]any  `json:"params,omitempty"`
}

// Operation is a single concrete operation instance produced by the generator
// and handed to a driver's Execute. Seq is monotonic per key so verifiers can
// reconstruct expected state and order.
type Operation struct {
	Type  OpType
	Key   int64
	Value []byte
	Seq   int64
	Args  map[string]any
}

// OpResult is what a driver returns from Execute. The runner — not the driver —
// measures wall-clock latency, so drivers must not time themselves. Observed
// carries any value/list read back, which correctness histories require.
type OpResult struct {
	Type     OpType
	Rows     int
	Observed any
	Err      error
}
