package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// requireCluster skips a test unless a kubectl-reachable cluster is up and the
// run is not in -short mode. This keeps the default `go test` cluster-free.
func requireCluster(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping kubernetes integration test in -short mode")
	}
	if !clusterAvailable() {
		t.Skip("skipping: no kubectl-reachable cluster available")
	}
}

// TestUpDownIntegration exercises the real kubectl path end to end. It is gated
// behind requireCluster so it never runs in CI's default fast path.
func TestUpDownIntegration(t *testing.T) {
	requireCluster(t)

	top := domain.Topology{
		Name:              "itest",
		Backend:           domain.BackendKubernetes,
		Target:            "redis",
		Nodes:             2,
		ReplicationFactor: 2,
	}
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		down, c := context.WithTimeout(context.Background(), 2*time.Minute)
		defer c()
		_ = p.Down(down, top)
	})

	spec, err := p.Up(ctx, top)
	if err != nil {
		t.Fatalf("Up: %v", err)
	}
	if len(spec.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(spec.Nodes))
	}
	for _, n := range spec.Nodes {
		if n.Control.Method != domain.ControlK8s {
			t.Errorf("node %s control method = %q", n.ID, n.Control.Method)
		}
		if n.Control.Namespace != "proofload-itest" {
			t.Errorf("node %s control namespace = %q", n.ID, n.Control.Namespace)
		}
		if n.Client == "" {
			t.Errorf("node %s has no client endpoint", n.ID)
		}
	}

	nodes, err := p.Nodes(ctx, top)
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("Nodes() = %d, want 2", len(nodes))
	}
}
