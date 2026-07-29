package k8s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/provision"
)

// rolloutTimeout bounds how long Up waits for the StatefulSet to become ready.
const rolloutTimeout = 3 * time.Minute

// provisioner implements provision.Provisioner over the kubectl CLI.
type provisioner struct{}

// New returns a Kubernetes-backed Provisioner.
func New() provision.Provisioner { return provisioner{} }

func init() { provision.Register(New()) }

// Backend reports the backend this provisioner implements.
func (provisioner) Backend() domain.Backend { return domain.BackendKubernetes }

// Up renders the manifests, applies them, waits for the StatefulSet rollout,
// and returns the resolved cluster. It is idempotent: `kubectl apply`
// reconciles an already-running topology without disruption.
func (p provisioner) Up(ctx context.Context, t domain.Topology) (domain.ClusterSpec, error) {
	m := buildModel(t)
	file, err := writeManifest(t, m)
	if err != nil {
		return domain.ClusterSpec{}, err
	}
	if _, err := runKubectl(ctx, "apply", "-f", file); err != nil {
		return domain.ClusterSpec{}, err
	}
	if err := rolloutStatus(ctx, m, rolloutTimeout); err != nil {
		return domain.ClusterSpec{}, err
	}
	return p.spec(t, m), nil
}

// spec resolves the model into a ClusterSpec.
func (provisioner) spec(t domain.Topology, m manifestModel) domain.ClusterSpec {
	rf := t.ReplicationFactor
	if rf == 0 {
		rf = m.Replicas
	}
	return domain.ClusterSpec{
		Nodes:             m.nodes(),
		ReplicationFactor: rf,
	}
}

// Down tears the topology down by deleting its namespace, which cascades to the
// Service and StatefulSet (and their pods).
func (provisioner) Down(ctx context.Context, t domain.Topology) error {
	_, err := runKubectl(ctx,
		"delete", "namespace", namespaceFor(t), "--ignore-not-found")
	return err
}

// Nodes returns current node handles without changing state, parsed from
// `kubectl get pods -o json`. Roles and endpoints are derived from each pod's
// StatefulSet ordinal so they stay consistent with Up.
func (provisioner) Nodes(ctx context.Context, t domain.Topology) ([]domain.Node, error) {
	m := buildModel(t)
	raw, err := runKubectl(ctx,
		"get", "pods", "-n", m.Namespace,
		"-l", "app="+m.Name, "-o", "json")
	if err != nil {
		return nil, err
	}
	items, err := parsePods(raw)
	if err != nil {
		return nil, fmt.Errorf("parse pods: %w", err)
	}
	return nodesFromPods(m, items), nil
}

// nodesFromPods maps live pods onto the model to build resolved nodes.
func nodesFromPods(m manifestModel, items []podItem) []domain.Node {
	var nodes []domain.Node
	for _, it := range items {
		pod := it.Metadata.Name
		nodes = append(nodes, domain.Node{
			ID:      pod,
			Role:    m.roleAt(podOrdinal(pod)),
			Client:  m.resolveClient(pod),
			Control: m.controlFor(pod),
		})
	}
	return nodes
}

// workDir is the per-topology directory holding the rendered manifests.
func workDir(t domain.Topology) string {
	return filepath.Join(".proofload", "k8s", t.Name)
}

// writeManifest renders the model and writes manifest.yaml, returning its path.
// Rendering is deterministic so repeated calls are stable.
func writeManifest(t domain.Topology, m manifestModel) (string, error) {
	dir := workDir(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create work dir %s: %w", dir, err)
	}
	yaml, err := render(m)
	if err != nil {
		return "", fmt.Errorf("render manifests: %w", err)
	}
	file := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(file, []byte(yaml), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", file, err)
	}
	return file, nil
}
