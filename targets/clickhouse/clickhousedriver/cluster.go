package clickhousedriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// Ensure chDriver satisfies both the required Driver port and the optional
// ClusterAware capability.
var (
	_ driver.Driver       = (*chDriver)(nil)
	_ driver.ClusterAware = (*chDriver)(nil)
)

// Nodes returns the QUERYABLE node handles for the configured cluster: the
// server replicas that expose a client endpoint. Coordination-only nodes such as
// clickhouse-keepers carry no client endpoint and are excluded, so convergence
// and staleness checks only read from nodes that actually serve queries. (The
// nemesis still targets keepers for fault injection via the resolved cluster
// spec, independently of this list.)
func (*chDriver) Nodes(cfg driver.Config) []domain.Node {
	out := make([]domain.Node, 0, len(cfg.Cluster.Nodes))
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n)
		}
	}
	return out
}

// ConsistencyLevels lists the consistency levels this target supports.
func (*chDriver) ConsistencyLevels() []string {
	out := make([]string, len(supportedConsistency))
	copy(out, supportedConsistency)
	return out
}

// ReadKeyFrom reads one key directly from a single node's client endpoint,
// bypassing normal cluster routing so verifiers can detect replica divergence
// and measure staleness. It opens a short-lived connection pinned to that one
// endpoint and reads with default consistency, so the answer comes from that
// server's own replica rather than a quorum of peers. This is what proves a
// killed replica's data survives on its peer and reconverges on restart.
func (*chDriver) ReadKeyFrom(ctx context.Context, node domain.Node, key int64) (domain.OpResult, error) {
	res := domain.OpResult{Type: "read"}
	if node.Client == "" {
		return res, fmt.Errorf("clickhousedriver: node %q has no client endpoint", node.ID)
	}

	opts, err := buildOptions(driver.Config{}, node.Client)
	if err != nil {
		return res, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return res, fmt.Errorf("clickhousedriver: connect to node %q: %w", node.ID, err)
	}
	defer conn.Close()

	var v string
	err = conn.QueryRow(ctx, readSQL, key).Scan(&v)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return res, nil
		}
		return res, fmt.Errorf("clickhousedriver: read key %d from node %q: %w", key, node.ID, err)
	}
	res.Observed = []byte(v)
	res.Rows = 1
	return res, nil
}
