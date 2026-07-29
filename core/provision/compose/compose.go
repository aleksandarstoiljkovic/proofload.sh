package compose

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/provision"
)

// healthTimeout bounds how long Up waits for all containers to become ready.
// Set generously: staggered multi-node clusters (e.g. a 3-node Cassandra ring
// that bootstraps one node at a time) can take several minutes. waitHealthy
// returns as soon as everything is ready, so fast clusters are unaffected.
const healthTimeout = 6 * time.Minute

// provisioner implements provision.Provisioner over the docker compose CLI.
type provisioner struct{}

// New returns a Compose-backed Provisioner.
func New() provision.Provisioner { return provisioner{} }

func init() { provision.Register(New()) }

// Backend reports the backend this provisioner implements.
func (provisioner) Backend() domain.Backend { return domain.BackendCompose }

// Up renders and starts the topology, waits for health, and returns the
// resolved cluster. It is idempotent: `docker compose up -d` reconciles an
// already-running project without disruption.
func (p provisioner) Up(ctx context.Context, t domain.Topology) (domain.ClusterSpec, error) {
	if t.Bundle != "" {
		return upBundle(ctx, t)
	}
	m := buildModel(t)
	file, err := writeProject(t, m)
	if err != nil {
		return domain.ClusterSpec{}, err
	}
	if _, err := runDocker(ctx, composeArgs(m.Project, file, "up", "-d", "--wait")...); err != nil {
		// --wait is best-effort; fall back to explicit polling regardless.
		_, _ = runDocker(ctx, composeArgs(m.Project, file, "up", "-d")...)
	}
	if err := waitHealthy(ctx, m, healthTimeout); err != nil {
		return domain.ClusterSpec{}, err
	}
	return p.spec(ctx, t, m, file), nil
}

// spec resolves published ports into a ClusterSpec.
func (provisioner) spec(ctx context.Context, t domain.Topology, m projectModel, file string) domain.ClusterSpec {
	rf := t.ReplicationFactor
	if rf == 0 {
		rf = len(m.Services)
	}
	spec := domain.ClusterSpec{ReplicationFactor: rf}
	for _, s := range m.Services {
		port := resolvePort(ctx, m.Project, file, s)
		spec.Nodes = append(spec.Nodes, domain.Node{
			ID:     s.ContainerName,
			Role:   s.Role,
			Client: fmt.Sprintf("127.0.0.1:%d", port),
			Control: domain.ControlEndpoint{
				Method: domain.ControlDocker,
				Ref:    s.ContainerName,
			},
		})
	}
	return spec
}

// Down tears the topology down and removes its volumes.
func (provisioner) Down(ctx context.Context, t domain.Topology) error {
	if t.Bundle != "" {
		return downBundle(ctx, t)
	}
	m := buildModel(t)
	file, err := writeProject(t, m)
	if err != nil {
		return err
	}
	_, err = runDocker(ctx, composeArgs(m.Project, file, "down", "-v")...)
	return err
}

// Nodes returns current node handles without changing state, parsed from
// `docker compose ps`.
func (provisioner) Nodes(ctx context.Context, t domain.Topology) ([]domain.Node, error) {
	if t.Bundle != "" {
		man, err := readBundleManifest(t.Bundle)
		if err != nil {
			return nil, err
		}
		return nodesBundle(ctx, t, man)
	}
	m := buildModel(t)
	file, err := writeProject(t, m)
	if err != nil {
		return nil, err
	}
	raw, err := runDocker(ctx, composeArgs(m.Project, file, "ps", "--format", "json")...)
	if err != nil {
		return nil, err
	}
	entries, err := parsePS(raw)
	if err != nil {
		return nil, fmt.Errorf("parse compose ps: %w", err)
	}
	return nodesFromPS(ctx, m, file, entries), nil
}

// nodesFromPS maps ps entries onto the model to build resolved nodes.
func nodesFromPS(ctx context.Context, m projectModel, file string, entries []psEntry) []domain.Node {
	byService := map[string]psEntry{}
	for _, e := range entries {
		byService[e.Service] = e
	}
	var nodes []domain.Node
	for _, s := range m.Services {
		e, ok := byService[s.Key]
		if !ok {
			continue
		}
		port := resolvePort(ctx, m.Project, file, s)
		nodes = append(nodes, domain.Node{
			ID:     e.Name,
			Role:   s.Role,
			Client: fmt.Sprintf("127.0.0.1:%d", port),
			Control: domain.ControlEndpoint{
				Method: domain.ControlDocker,
				Ref:    e.Name,
			},
		})
	}
	return nodes
}

// workDir is the per-topology directory holding the rendered compose file.
func workDir(t domain.Topology) string {
	return filepath.Join(".proofload", "compose", t.Name)
}

// writeProject renders the model and writes docker-compose.yml, returning its
// path. Rendering is deterministic so repeated calls are stable.
func writeProject(t domain.Topology, m projectModel) (string, error) {
	dir := workDir(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create work dir %s: %w", dir, err)
	}
	yaml, err := render(m)
	if err != nil {
		return "", fmt.Errorf("render compose: %w", err)
	}
	file := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(file, []byte(yaml), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", file, err)
	}
	return file, nil
}
