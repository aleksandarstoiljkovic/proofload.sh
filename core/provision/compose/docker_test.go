package compose

import (
	"context"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// requireDocker skips a test unless a docker daemon is reachable and the run is
// not in -short mode. This keeps the default `go test` daemon-free.
func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping docker integration test in -short mode")
	}
	if !dockerAvailable() {
		t.Skip("skipping: docker daemon not available")
	}
}

// TestUpDownIntegration exercises the real docker path end to end. It is gated
// behind requireDocker so it never runs in CI's default fast path.
func TestUpDownIntegration(t *testing.T) {
	requireDocker(t)

	top := domain.Topology{
		Name:              "itest",
		Backend:           domain.BackendCompose,
		Target:            "redis",
		Nodes:             2,
		ReplicationFactor: 2,
	}
	p := New()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		down, c := context.WithTimeout(context.Background(), time.Minute)
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
		if n.Control.Method != domain.ControlDocker {
			t.Errorf("node %s control method = %q", n.ID, n.Control.Method)
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
