package redisdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/redis/go-redis/v9"
)

// consistencyLevels is the fixed set of levels this target exposes, ordered
// weakest to strongest. Kept as a package var so ConsistencyLevels and tests
// share one definition.
var consistencyLevels = []string{consNone, consWait}

// Ensure redisDriver satisfies both the required Driver port and the optional
// ClusterAware capability.
var (
	_ driver.Driver       = (*redisDriver)(nil)
	_ driver.ClusterAware = (*redisDriver)(nil)
)

// Nodes returns the QUERYABLE node handles for the configured cluster: the data
// nodes (primary and replicas) that expose a client endpoint. Coordination-only
// nodes such as Redis Sentinels carry no client endpoint and are excluded, so
// convergence and staleness checks only read from nodes that actually serve
// queries. (The nemesis still targets sentinels for fault injection via the
// resolved cluster spec, independently of this list.)
func (*redisDriver) Nodes(cfg driver.Config) []domain.Node {
	out := make([]domain.Node, 0, len(cfg.Cluster.Nodes))
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n)
		}
	}
	return out
}

// ConsistencyLevels lists the levels this target supports.
func (*redisDriver) ConsistencyLevels() []string {
	out := make([]string, len(consistencyLevels))
	copy(out, consistencyLevels)
	return out
}

// ReadKeyFrom reads one key directly from a specific node's client endpoint,
// bypassing normal routing so verifiers can detect replica divergence and
// measure staleness. It opens a short-lived client to that node; the password
// comes from the environment, matching how Connect resolves credentials. A
// missing key is reported as an empty (nil) observation, not an error.
func (*redisDriver) ReadKeyFrom(ctx context.Context, node domain.Node, key int64) (domain.OpResult, error) {
	res := domain.OpResult{Type: "read"}
	if node.Client == "" {
		return res, fmt.Errorf("redisdriver: node %q has no client endpoint", node.ID)
	}

	client := redis.NewClient(optionsFor(resolveOptions(node.Client, nil)))
	defer client.Close()

	v, err := client.Get(ctx, keyName(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, nil
		}
		return res, fmt.Errorf("redisdriver: read key %d from node %q: %w", key, node.ID, err)
	}
	res.Observed = v
	res.Rows = 1
	return res, nil
}
