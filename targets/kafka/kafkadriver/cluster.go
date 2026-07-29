package kafkadriver

import (
	"context"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// cluster.go implements the optional driver.ClusterAware capability. Kafka
// exposes a multi-broker topology, so Nodes and ConsistencyLevels are
// meaningful; per-node key reads are not, because Kafka is a log rather than a
// keyed store, so ReadKeyFrom reports an explicit unsupported error.

// Nodes returns the resolved broker handles for the configured cluster. Only
// nodes with a client endpoint are returned: every Kafka broker is directly
// addressable by clients (there is no coordination-only tier as in ClickHouse's
// Keeper), so a node without a Client address is not a usable broker handle.
func (*kafkaDriver) Nodes(cfg driver.Config) []domain.Node {
	out := make([]domain.Node, 0, len(cfg.Cluster.Nodes))
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n)
		}
	}
	return out
}

// ConsistencyLevels lists the delivery semantics (acks levels) this target
// supports, weakest to strongest.
func (*kafkaDriver) ConsistencyLevels() []string {
	out := make([]string, len(consistencyLevels))
	copy(out, consistencyLevels)
	return out
}

// ReadKeyFrom is not meaningful for Kafka: there is no per-key point read from a
// specific broker. It returns a clear unsupported error so verifiers relying on
// divergence/staleness probes fail loudly instead of silently misbehaving. Use
// the kafka-log verify model (see verify.go) for Kafka correctness instead.
func (*kafkaDriver) ReadKeyFrom(_ context.Context, _ domain.Node, _ int64) (domain.OpResult, error) {
	return domain.OpResult{}, errReadKeyUnsupported
}
