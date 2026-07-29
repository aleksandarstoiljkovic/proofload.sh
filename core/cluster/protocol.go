// Package cluster provides the HTTP coordinator/worker transport for distributed
// load generation. One machine runs a Coordinator; N load-client machines each
// run a Worker that Joins (receiving the shared RunSpec and a synchronized start
// time T0), runs the SAME load locally against the target, then Submits its
// WorkerReport plus an opaque encoded-histogram blob. The Coordinator aggregates
// the reports and collects the blobs; the caller Decodes+Merges the blobs via
// core/metrics for a lossless distributed histogram merge.
//
// This package deals only in transport, protocol, and aggregation. It never
// imports core/metrics and treats every histogram as an opaque []byte payload.
package cluster

import (
	"encoding/json"
	"net/http"
	"time"
)

// RunSpec is the load definition the coordinator broadcasts to every worker so
// they generate identical load. WorkerCount plus the assigned WorkerID let each
// worker shard the keyspace via workload.New(w, Seed, WorkerID, WorkerCount).
type RunSpec struct {
	Target        string        `json:"target"`
	Workload      string        `json:"workload"`
	Endpoints     []string      `json:"endpoints"`
	Consistency   string        `json:"consistency"`
	Connections   int           `json:"connections"`
	Warmup        time.Duration `json:"warmup"`
	Duration      time.Duration `json:"duration"`
	RateOpsPerSec int           `json:"rate_ops_per_sec"`
	Seed          int64         `json:"seed"`
	WorkerCount   int           `json:"worker_count"`
}

// WorkerReport is one worker's scalar summary of its measure phase. Latency
// distributions travel separately as an opaque histogram blob.
type WorkerReport struct {
	WorkerID    int   `json:"worker_id"`
	Total       int64 `json:"total"`
	Errors      int64 `json:"errors"`
	ClientBound bool  `json:"client_bound"`
}

// Result is the aggregated outcome of a distributed run. Reports and Histograms
// are index-aligned and ordered by WorkerID. Each Histograms entry is an opaque
// per-worker encoded blob; the caller Decodes+Merges them with core/metrics.
type Result struct {
	Reports    []WorkerReport `json:"reports"`
	Histograms [][]byte       `json:"histograms"`
}

// joinRequest is the /join body. RequestedID < 0 means "assign me a free id";
// an in-range free id is honored, otherwise the coordinator picks a free slot.
type joinRequest struct {
	RequestedID int `json:"requested_id"`
}

// joinResponse is the /join reply: the assigned id, the shared spec, and the
// synchronized start time T0 every worker uses to begin its measure phase.
type joinResponse struct {
	WorkerID int       `json:"worker_id"`
	Spec     RunSpec   `json:"spec"`
	StartAt  time.Time `json:"start_at"`
}

// submitRequest is the /submit body: a report plus an opaque histogram blob.
// Histogram marshals as base64 in JSON, preserving the blob byte-for-byte.
type submitRequest struct {
	Report    WorkerReport `json:"report"`
	Histogram []byte       `json:"histogram"`
}

// writeJSON writes v as an application/json response, ignoring encode errors
// (the connection is the only place such an error could surface).
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
