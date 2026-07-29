package driver

import (
	"context"

	"github.com/proofload/proofload/core/domain"
)

// ClusterAware is an optional capability for targets that expose a multi-node
// topology. It enables per-node direct reads (convergence/staleness checks) and
// consistency-level sweeps. Drivers that don't implement it are treated as
// single-endpoint for verification purposes.
type ClusterAware interface {
	// Nodes returns the resolved node handles for the configured cluster.
	Nodes(cfg Config) []domain.Node
	// ReadKeyFrom reads a key from ONE specific node/replica, bypassing normal
	// routing, so verifiers can detect divergence and measure staleness.
	ReadKeyFrom(ctx context.Context, node domain.Node, key int64) (domain.OpResult, error)
	// ConsistencyLevels lists the levels this target supports.
	ConsistencyLevels() []string
}

// RunArtifacts locates the on-disk products of a completed run that a Verifier
// consumes (history, recorded expectations, resolved config).
type RunArtifacts struct {
	Dir             string
	HistoryPath     string // Elle/Porcupine history (correctness runs)
	ExpectationPath string // recorded committed writes (reconciliation)
	Cfg             Config
}

// Verifier is an optional capability that checks correctness after a run. Most
// targets rely on the built-in core/verify models and only supply read-back
// primitives via ClusterAware; a target implements Verifier directly only when
// it needs bespoke checking logic.
type Verifier interface {
	Model() domain.VerifyModel
	Verify(ctx context.Context, art RunArtifacts) (domain.VerifyReport, error)
}

// FaultController is an optional capability that injects and heals faults on a
// node. It is usually a thin wrapper over the target's faults/fault.sh, which in
// turn drives the node's control endpoint (k8s/docker/ssh).
type FaultController interface {
	Inject(ctx context.Context, f domain.Fault, target domain.Node) error
	Heal(ctx context.Context, target domain.Node) error
}
