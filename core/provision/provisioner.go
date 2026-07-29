// Package provision defines the Provisioner port: the seam that lets proofload
// own a cluster topology's lifecycle on a pluggable backend (Kubernetes for
// chaos-capable, reproducible clusters; Compose for a fast local loop; External
// for remote/managed targets that proofload only connects to). Backends live in
// subpackages and register themselves so the orchestrator can select one by name.
package provision

import (
	"context"
	"fmt"
	"sort"

	"github.com/proofload/proofload/core/domain"
)

// Provisioner owns the lifecycle of one cluster topology on a specific backend.
// Implementations must be idempotent: calling Up on an already-ready topology
// returns the current ClusterSpec without disruption.
type Provisioner interface {
	// Backend reports which backend this provisioner implements.
	Backend() domain.Backend
	// Up brings the topology to a ready state and returns the resolved runtime
	// cluster (client endpoints + control endpoints for each node).
	Up(ctx context.Context, t domain.Topology) (domain.ClusterSpec, error)
	// Down tears the topology down and releases resources.
	Down(ctx context.Context, t domain.Topology) error
	// Nodes returns current node handles without changing state, for use by
	// verification (direct reads) and the nemesis (fault targets).
	Nodes(ctx context.Context, t domain.Topology) ([]domain.Node, error)
}

// registry maps a backend to its provisioner implementation.
var registry = map[domain.Backend]Provisioner{}

// Register makes a backend selectable by the orchestrator.
func Register(p Provisioner) {
	if p == nil {
		panic("provision: Register called with nil Provisioner")
	}
	b := p.Backend()
	if _, dup := registry[b]; dup {
		panic(fmt.Sprintf("provision: duplicate registration for backend %q", b))
	}
	registry[b] = p
}

// Get returns the provisioner for a backend.
func Get(b domain.Backend) (Provisioner, error) {
	p, ok := registry[b]
	if !ok {
		return nil, fmt.Errorf("provision: no backend registered as %q (have %v)", b, Backends())
	}
	return p, nil
}

// Backends returns the sorted list of registered backend names.
func Backends() []string {
	out := make([]string, 0, len(registry))
	for b := range registry {
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}
