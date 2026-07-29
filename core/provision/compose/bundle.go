package compose

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// bundleManifestFile is the per-bundle manifest naming the compose services that
// become proofload nodes, read from a target's cluster/ directory.
const bundleManifestFile = "proofload-cluster.json"

// bundleNode is one entry of a bundle manifest. Services with a ClientPort are
// load endpoints (their host port is resolved via `docker compose port`);
// services without one (e.g. keepers) are coordination nodes that are still
// fault-targetable by ID.
type bundleNode struct {
	Service    string `json:"service"`
	Role       string `json:"role"`
	ClientPort int    `json:"client_port"`
}

type bundleManifest struct {
	Nodes []bundleNode `json:"nodes"`
}

// bundleProject and bundleFile derive the compose project name and file path for
// a target-supplied bundle.
func bundleProject(t domain.Topology) string { return "proofload-" + t.Name }
func bundleFile(t domain.Topology) string {
	return filepath.Join(t.Bundle, "docker-compose.yml")
}

// readBundleManifest loads the node manifest from a bundle directory.
func readBundleManifest(dir string) (bundleManifest, error) {
	var m bundleManifest
	b, err := os.ReadFile(filepath.Join(dir, bundleManifestFile))
	if err != nil {
		return m, fmt.Errorf("read bundle manifest: %w", err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("parse bundle manifest: %w", err)
	}
	if len(m.Nodes) == 0 {
		return m, fmt.Errorf("bundle manifest %s has no nodes", dir)
	}
	return m, nil
}

// upBundle starts a target-supplied compose bundle verbatim (so its relative
// config mounts resolve against the bundle directory), waits for health, and
// resolves the cluster from the bundle manifest.
func upBundle(ctx context.Context, t domain.Topology) (domain.ClusterSpec, error) {
	man, err := readBundleManifest(t.Bundle)
	if err != nil {
		return domain.ClusterSpec{}, err
	}
	proj, file := bundleProject(t), bundleFile(t)
	if _, err := runDocker(ctx, composeArgs(proj, file, "up", "-d", "--wait")...); err != nil {
		if _, err2 := runDocker(ctx, composeArgs(proj, file, "up", "-d")...); err2 != nil {
			return domain.ClusterSpec{}, err2
		}
	}
	if err := waitHealthyBundle(ctx, proj, file, man, healthTimeout); err != nil {
		return domain.ClusterSpec{}, err
	}
	nodes, err := nodesBundle(ctx, t, man)
	if err != nil {
		return domain.ClusterSpec{}, err
	}
	rf := t.ReplicationFactor
	if rf == 0 {
		rf = len(nodes)
	}
	return domain.ClusterSpec{Nodes: nodes, ReplicationFactor: rf}, nil
}

// downBundle tears down a bundle project and its volumes.
func downBundle(ctx context.Context, t domain.Topology) error {
	_, err := runDocker(ctx, composeArgs(bundleProject(t), bundleFile(t), "down", "-v")...)
	return err
}

// nodesBundle resolves the bundle manifest into live nodes: control endpoints
// from `docker compose ps`, client endpoints from `docker compose port`.
func nodesBundle(ctx context.Context, t domain.Topology, man bundleManifest) ([]domain.Node, error) {
	proj, file := bundleProject(t), bundleFile(t)
	raw, err := runDocker(ctx, composeArgs(proj, file, "ps", "--format", "json")...)
	if err != nil {
		return nil, err
	}
	entries, err := parsePS(raw)
	if err != nil {
		return nil, fmt.Errorf("parse compose ps: %w", err)
	}
	byService := map[string]psEntry{}
	for _, e := range entries {
		byService[e.Service] = e
	}
	var nodes []domain.Node
	for _, n := range man.Nodes {
		name := n.Service
		if e, ok := byService[n.Service]; ok && e.Name != "" {
			name = e.Name
		}
		node := domain.Node{
			ID:      name,
			Role:    domain.NodeRole(n.Role),
			Control: domain.ControlEndpoint{Method: domain.ControlDocker, Ref: name},
		}
		if n.ClientPort > 0 {
			port := resolveNamedPort(ctx, proj, file, n.Service, n.ClientPort)
			node.Client = fmt.Sprintf("127.0.0.1:%d", port)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// waitHealthyBundle polls the bundle's services until all are ready.
func waitHealthyBundle(ctx context.Context, proj, file string, man bundleManifest, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pending, err := bundlePending(ctx, proj, file, man)
		if err == nil && len(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for: %v", timeout, pending)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// bundlePending returns the bundle services not yet ready.
func bundlePending(ctx context.Context, proj, file string, man bundleManifest) ([]string, error) {
	raw, err := runDocker(ctx, composeArgs(proj, file, "ps", "--format", "json")...)
	if err != nil {
		return nil, err
	}
	entries, err := parsePS(raw)
	if err != nil {
		return nil, err
	}
	byService := map[string]psEntry{}
	for _, e := range entries {
		byService[e.Service] = e
	}
	var pending []string
	for _, n := range man.Nodes {
		e, ok := byService[n.Service]
		if !ok || !entryReady(e) {
			pending = append(pending, n.Service)
		}
	}
	return pending, nil
}

// entryReady reports whether a ps entry is healthy (or running with no
// healthcheck).
func entryReady(e psEntry) bool {
	if e.Health != "" {
		return e.Health == "healthy"
	}
	return e.State == "running"
}

// resolveNamedPort resolves the published host port for a service's container
// port, falling back to the container port when the query fails.
func resolveNamedPort(ctx context.Context, proj, file, service string, containerPort int) int {
	out, err := runDocker(ctx, composeArgs(proj, file, "port", service, fmt.Sprint(containerPort))...)
	if err != nil {
		return containerPort
	}
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == ':' {
			if p := parsePort(out[i+1:]); p > 0 {
				return p
			}
			break
		}
	}
	return containerPort
}
