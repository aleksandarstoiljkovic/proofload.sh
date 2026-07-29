package cassandradriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// Nodes returns the QUERYABLE node handles for the configured cluster: those
// that expose a client endpoint. Every Cassandra node serves queries, so all
// three carry a client endpoint here; the filter is kept for uniformity with
// the other targets (e.g. ClickHouse excludes coordination-only keepers) so
// convergence and staleness checks only ever read from queryable nodes.
func (*cassDriver) Nodes(cfg driver.Config) []domain.Node {
	out := make([]domain.Node, 0, len(cfg.Cluster.Nodes))
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n)
		}
	}
	return out
}

// ConsistencyLevels lists the consistency levels this target supports.
func (*cassDriver) ConsistencyLevels() []string {
	out := make([]string, len(supportedConsistency))
	copy(out, supportedConsistency)
	return out
}

// ReadKeyFrom reads one key directly from a single node, bypassing normal
// token-aware routing so verifiers can detect replica divergence and measure
// staleness. It opens a short-lived session pinned to that one host (a
// single-host cluster config plus a WhiteListHostFilter, so gossip-discovered
// peers are not dialed) and reads at consistency ONE, which forces the answer to
// come from that node's own replica rather than a quorum of peers.
func (*cassDriver) ReadKeyFrom(ctx context.Context, node domain.Node, key int64) (domain.OpResult, error) {
	res := domain.OpResult{Type: "read"}
	if node.Client == "" {
		return res, fmt.Errorf("cassandradriver: node %q has no client endpoint", node.ID)
	}

	opts, err := resolveOptions(driver.Config{Consistency: "one"}, keyspaceName)
	if err != nil {
		return res, err
	}
	cluster := buildCluster([]string{node.Client}, opts)
	cluster.HostFilter = gocql.WhiteListHostFilter(node.Client)
	cluster.DisableInitialHostLookup = true

	session, err := cluster.CreateSession()
	if err != nil {
		return res, fmt.Errorf("cassandradriver: connect to node %q: %w", node.ID, err)
	}
	defer session.Close()

	var v []byte
	err = session.Query(readCQL, key).WithContext(ctx).Scan(&v)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return res, nil
		}
		return res, fmt.Errorf("cassandradriver: read key %d from node %q: %w", key, node.ID, err)
	}
	res.Observed = v
	res.Rows = 1
	return res, nil
}
