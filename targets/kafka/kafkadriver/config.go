package kafkadriver

import (
	"fmt"
	"strings"

	"github.com/proofload/proofload/core/driver"
)

// Defaults applied when neither the workload/target params nor the cluster spec
// supply a value.
const (
	defaultTopic       = "proofload"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultKafkaPort   = "9092"
	seqHeaderKey       = "pl-seq"
	defaultBatchSize   = 1
)

// ackMode is the delivery guarantee for a produce, parsed from a consistency
// level. It is target-internal so the mapping to franz-go acks lives in one
// place and can be unit tested without a broker (see clientOpts).
type ackMode int

const (
	ackAll    ackMode = iota // acks=all (-1): wait for all in-sync replicas
	ackLeader                // acks=1: wait for the partition leader only
	ackNone                  // acks=0: fire and forget
)

// consistencyLevels is the fixed set of delivery semantics this target exposes,
// ordered weakest to strongest. Shared by ConsistencyLevels and parseAcks.
var consistencyLevels = []string{"acks=0", "acks=1", "acks=all"}

// parseAcks maps a driver.Config.Consistency string onto an ackMode. The empty
// string defaults to the strongest level (acks=all), matching the target's
// documented default. Unknown levels are rejected so misconfiguration fails
// fast rather than silently weakening durability.
func parseAcks(consistency string) (ackMode, error) {
	switch strings.ToLower(strings.TrimSpace(consistency)) {
	case "", "acks=all", "all", "-1":
		return ackAll, nil
	case "acks=1", "1", "leader":
		return ackLeader, nil
	case "acks=0", "0", "none":
		return ackNone, nil
	default:
		return 0, fmt.Errorf("kafkadriver: unsupported consistency %q (want one of %v)", consistency, consistencyLevels)
	}
}

// topicConfig is the resolved topic to ensure and produce/consume against.
type topicConfig struct {
	Name        string
	Partitions  int32
	Replication int16
}

// resolvedConfig is the fully resolved runtime view a Conn/Schema call needs.
type resolvedConfig struct {
	brokers    []string
	topic      topicConfig
	acks       ackMode
	idempotent bool   // EOS idempotent producer (implied by txnID or acks=all)
	txnID      string // non-empty enables transactional (read-committed EOS) writes
	group      string // non-empty makes the client a consumer-group member
	batchSize  int    // records produced per Execute (>1 = batched produce)
}

// resolve derives the runtime config from a driver.Config. It is pure apart from
// reading cfg, so the params → config mapping is unit testable without a broker.
func resolve(cfg driver.Config) (resolvedConfig, error) {
	acks, err := parseAcks(cfg.Consistency)
	if err != nil {
		return resolvedConfig{}, err
	}

	rc := resolvedConfig{
		brokers:   brokersFrom(cfg),
		topic:     topicConfigFrom(cfg),
		acks:      acks,
		group:     firstNonEmpty(paramString(cfg.Params, "group"), paramString(cfg.Params, "consumer_group")),
		txnID:     firstNonEmpty(paramString(cfg.Params, "transactional_id"), paramString(cfg.Params, "txn_id")),
		batchSize: batchSizeFrom(cfg),
	}

	// Transactional writes require an idempotent producer at acks=all; force
	// those invariants so an EOS toggle can't be combined with a weaker level.
	if rc.txnID != "" {
		rc.acks = ackAll
		rc.idempotent = true
	} else {
		// Idempotence is only valid at acks=all. Default it on there, but let a
		// workload force it off (e.g. to measure the non-idempotent hot path).
		rc.idempotent = rc.acks == ackAll && paramBool(cfg.Params, "idempotent", true)
	}
	return rc, nil
}

// topicConfigFrom resolves the topic name, partition count, and replication
// factor. Replication defaults to the cluster's configured factor when present.
func topicConfigFrom(cfg driver.Config) topicConfig {
	repl := defaultReplication
	if cfg.Cluster.ReplicationFactor > 0 {
		repl = cfg.Cluster.ReplicationFactor
	}
	return topicConfig{
		Name:        firstNonEmpty(paramString(cfg.Params, "topic"), defaultTopic),
		Partitions:  int32(paramInt(cfg.Params, "partitions", defaultPartitions)),
		Replication: int16(paramInt(cfg.Params, "replication_factor", repl)),
	}
}

// batchSizeFrom resolves how many records a single produce Execute emits. It is
// clamped to at least 1 so a missing or nonsensical value degrades to the
// classic one-record-per-op behaviour rather than producing nothing.
func batchSizeFrom(cfg driver.Config) int {
	n := paramInt(cfg.Params, "batch_size", defaultBatchSize)
	if n < 1 {
		return 1
	}
	return n
}

// brokersFrom resolves the seed broker list: explicit endpoints first, then the
// cluster's node client addresses, then a localhost default for local dev/tests.
func brokersFrom(cfg driver.Config) []string {
	if len(cfg.Endpoints) > 0 {
		return cfg.Endpoints
	}
	var out []string
	for _, n := range cfg.Cluster.Nodes {
		if n.Client != "" {
			out = append(out, n.Client)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:" + defaultKafkaPort}
	}
	return out
}
