// Package driver defines the ports a database/broker target implements to plug
// into proofload, plus a registry. Implementing Driver + Conn is the minimum to
// add a new target; ClusterAware, Verifier, and FaultController are optional
// capabilities layered on top. Everything else (timing, concurrency, histograms,
// scheduling, distribution, storage, reporting) is provided by core.
package driver

import (
	"context"

	"github.com/proofload/proofload/core/domain"
)

// Config is the resolved connection configuration handed to a driver. It is
// backend-agnostic: whether the cluster was provisioned by proofload or is a
// remote/managed service, the driver sees the same shape.
type Config struct {
	Endpoints   []string // client-facing host:port list
	Consistency string   // level for this run, e.g. "QUORUM", "serializable"
	Cluster     domain.ClusterSpec
	Params      map[string]any // target-specific knobs from target.yaml/workload
}

// Driver is the factory for connections to one kind of target. A target's
// engine/main.go constructs one Driver and registers it.
type Driver interface {
	// Name is the target identifier, matching the targets/<name>/ directory.
	Name() string
	// Schema applies any one-time setup (DDL, topics, keyspaces) before load.
	Schema(ctx context.Context, cfg Config, w domain.Workload) error
	// Connect opens a single connection/session. The runner opens many to reach
	// the requested concurrency; drivers should not pool internally.
	Connect(ctx context.Context, cfg Config) (Conn, error)
}

// Conn is a single connection/session used by one runner goroutine.
type Conn interface {
	// Prepare sets up per-connection state (prepared/bound statements) for the
	// workload. Called once after Connect, before the first Execute.
	Prepare(ctx context.Context, w domain.Workload) error
	// Execute runs exactly one operation. It must NOT measure latency — the
	// runner times the call with a monotonic clock. Errors are returned inside
	// the OpResult so the runner can bucket them per operation type.
	Execute(ctx context.Context, op domain.Operation) domain.OpResult
	// Close releases the connection.
	Close() error
}
