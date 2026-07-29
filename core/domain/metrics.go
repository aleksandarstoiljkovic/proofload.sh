package domain

import "time"

// Percentiles is a latency summary in milliseconds derived from an HdrHistogram.
// Percentiles are never averaged across sources; they are always recomputed from
// merged histograms (see core/metrics).
type Percentiles struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	P9999 float64 `json:"p9999_ms"`
	Max   float64 `json:"max_ms"`
}

// LatencySnapshot is one interval (typically one second) of the run time-series,
// persisted to Parquet and optionally streamed to a TSDB.
type LatencySnapshot struct {
	T          time.Time   `json:"t"`
	OpType     OpType      `json:"op"`
	Throughput float64     `json:"throughput"`
	InFlight   int         `json:"in_flight"`
	Errors     int64       `json:"errors"`
	Pct        Percentiles `json:"pct"`
}

// RunResult is the final aggregate for one phase of one run.
type RunResult struct {
	RunID      RunID                  `json:"run_id"`
	Phase      Phase                  `json:"phase"`
	Total      int64                  `json:"total"`
	Errors     int64                  `json:"errors"`
	Duration   time.Duration          `json:"duration"`
	Throughput float64                `json:"throughput"`
	Overall    Percentiles            `json:"overall"`
	ByOp       map[OpType]Percentiles `json:"by_op"`
	// ClientBound is set when the saturation guard determined the load client —
	// not the target — was the bottleneck, making the target numbers invalid.
	ClientBound bool `json:"client_bound"`
}
