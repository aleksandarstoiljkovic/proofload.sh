package verify_test

import (
	"context"
	"sync"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// clusterFake is a tiny two-node ClusterAware driver used to exercise the
// convergence path without any external services. Normal read-back (Execute)
// always returns the converged value, so loss/corruption stay clean; the
// convergence check sees node 0 report a stale value for its first
// staleReads reads before catching up.
type clusterFake struct {
	nodes      []domain.Node
	value      []byte // converged value on every node
	stale      []byte // node 0's value until it catches up
	staleReads int    // ReadKeyFrom calls to node 0 that return stale

	mu    sync.Mutex
	reads map[string]int
}

func newClusterFake(value, stale []byte, staleReads int) *clusterFake {
	return &clusterFake{
		nodes: []domain.Node{
			{ID: "n0", Role: domain.RoleReplica},
			{ID: "n1", Role: domain.RoleReplica},
		},
		value:      value,
		stale:      stale,
		staleReads: staleReads,
		reads:      make(map[string]int),
	}
}

func (c *clusterFake) Name() string { return "cluster-fake" }

func (c *clusterFake) Schema(context.Context, driver.Config, domain.Workload) error { return nil }

func (c *clusterFake) Connect(context.Context, driver.Config) (driver.Conn, error) {
	return c, nil
}

func (c *clusterFake) Prepare(context.Context, domain.Workload) error { return nil }

// Execute serves the routed read-back with the converged value.
func (c *clusterFake) Execute(context.Context, domain.Operation) domain.OpResult {
	return domain.OpResult{Rows: 1, Observed: c.value}
}

func (c *clusterFake) Close() error { return nil }

func (c *clusterFake) Nodes(driver.Config) []domain.Node { return c.nodes }

func (c *clusterFake) ConsistencyLevels() []string { return []string{"ONE", "QUORUM"} }

// ReadKeyFrom returns node 0's stale value for its first staleReads reads, then
// the converged value; node 1 is always converged.
func (c *clusterFake) ReadKeyFrom(_ context.Context, node domain.Node, _ int64) (domain.OpResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads[node.ID]++
	if node.ID == c.nodes[0].ID && c.reads[node.ID] <= c.staleReads {
		return domain.OpResult{Observed: c.stale}, nil
	}
	return domain.OpResult{Observed: c.value}, nil
}
