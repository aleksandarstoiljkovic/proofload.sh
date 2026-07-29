package pgdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// consistencyLevels is the fixed set of isolation levels this target exposes,
// ordered weakest to strongest. Kept as a package var so Nodes/ReadKeyFrom and
// tests share one definition.
var consistencyLevels = []string{"read-committed", "repeatable-read", "serializable"}

// Ensure pgDriver satisfies both the required Driver port and the optional
// ClusterAware capability.
var (
	_ driver.Driver       = (*pgDriver)(nil)
	_ driver.ClusterAware = (*pgDriver)(nil)
)

// Nodes returns the QUERYABLE node handles for the configured cluster: those
// exposing a client endpoint. Coordination-only nodes (with no client endpoint)
// are excluded so convergence/staleness checks only read queryable servers.
func (*pgDriver) Nodes(cfg driver.Config) []domain.Node {
	out := make([]domain.Node, 0, len(cfg.Cluster.Nodes))
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n)
		}
	}
	return out
}

// ConsistencyLevels lists the isolation levels this target supports.
func (*pgDriver) ConsistencyLevels() []string {
	out := make([]string, len(consistencyLevels))
	copy(out, consistencyLevels)
	return out
}

// ReadKeyFrom reads one key directly from a specific node's client endpoint,
// bypassing normal routing so verifiers can detect replica divergence and
// measure staleness. It opens a short-lived connection to that node; connection
// options (user/password/dbname) come from the environment, matching how the
// provisioner and Connect resolve credentials.
func (*pgDriver) ReadKeyFrom(ctx context.Context, node domain.Node, key int64) (domain.OpResult, error) {
	res := domain.OpResult{Type: "read"}
	if node.Client == "" {
		return res, fmt.Errorf("pgdriver: node %q has no client endpoint", node.ID)
	}

	dsn := buildDSN(node.Client, resolveOptions(nil))
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return res, fmt.Errorf("pgdriver: connect to node %q: %w", node.ID, err)
	}
	defer conn.Close(ctx)

	var v []byte
	err = conn.QueryRow(ctx, statements()[stmtRead], key).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, nil
		}
		return res, fmt.Errorf("pgdriver: read key %d from node %q: %w", key, node.ID, err)
	}
	res.Observed = v
	res.Rows = 1
	return res, nil
}
