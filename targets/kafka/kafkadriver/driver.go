// Package kafkadriver implements the proofload driver.Driver, driver.Conn,
// driver.ClusterAware, and driver.Verifier capabilities for Apache Kafka using
// franz-go (kgo).
//
// Role model: Kafka is a log, not a request/response store, so operation types
// are treated as roles on one kgo.Client per Conn:
//
//	produce | w        – synchronously produce one record to the topic. The key
//	                     is the decimal form of op.Key, the value is op.Value, and
//	                     op.Seq is carried in a header. ProduceSync is used so the
//	                     runner times the real end-of-ack latency.
//	produce_batch | wb – produce params.batch_size records in a single ProduceSync
//	                     call, so one Execute amortises the round-trip over a whole
//	                     batch and throughput reflects Kafka's real batched ingest
//	                     (100k+ msg/s) rather than the ~one-record-per-RTT rate of
//	                     a single synchronous produce. Rows is set to batch_size so
//	                     the run summary counts records, not batches. Each record
//	                     gets a distinct, deterministic key/seq (see batchRecordAt)
//	                     so the kafka-log verifier still holds. A plain "produce"
//	                     op also honours params.batch_size, so batching can be
//	                     enabled without changing op labels.
//	consume | r        – poll exactly one record from a maintained consumer and
//	                     return its value in OpResult.Observed (consume throughput
//	                     / e2e latency).
//
// Consistency maps to Kafka acks: "acks=all" (default), "acks=1", "acks=0".
// Idempotent and transactional (EOS) producing are toggled via params
// (idempotent, transactional_id). See config.go for the full mapping.
//
// Topic administration (Schema) uses a kmsg CreateTopics request issued through
// the kgo client rather than the separate kadm module, so the target adds no
// dependency beyond the already-required franz-go client.
package kafkadriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kafkaDriver is the Kafka driver.Driver implementation.
type kafkaDriver struct{}

// New returns a Kafka driver. The engine's main package registers it.
func New() driver.Driver { return &kafkaDriver{} }

// Ensure kafkaDriver satisfies the required Driver port plus the optional
// ClusterAware and Verifier capabilities.
var (
	_ driver.Driver       = (*kafkaDriver)(nil)
	_ driver.ClusterAware = (*kafkaDriver)(nil)
	_ driver.Verifier     = (*kafkaDriver)(nil)
)

// Name implements driver.Driver.
func (*kafkaDriver) Name() string { return "kafka" }

// Schema ensures the configured topic exists with the requested partition count
// and replication factor. It opens a short-lived admin client, issues a
// CreateTopics request, and treats "already exists" as success so it is safe to
// call repeatedly.
func (d *kafkaDriver) Schema(ctx context.Context, cfg driver.Config, _ domain.Workload) error {
	rc, err := resolve(cfg)
	if err != nil {
		return err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(rc.brokers...))
	if err != nil {
		return fmt.Errorf("kafkadriver: admin client: %w", err)
	}
	defer cl.Close()
	return ensureTopic(ctx, cl, rc.topic)
}

// Connect opens one kgo.Client configured as a producer (and, when a consumer
// group is configured, a group consumer). The runner opens many such
// connections to reach the requested concurrency.
func (d *kafkaDriver) Connect(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	rc, err := resolve(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(clientOpts(rc)...)
	if err != nil {
		return nil, fmt.Errorf("kafkadriver: connect: %w", err)
	}
	return &kafkaConn{
		cl:            cl,
		topic:         rc.topic.Name,
		transactional: rc.txnID != "",
		grouped:       rc.group != "",
		batchSize:     rc.batchSize,
	}, nil
}

// clientOpts renders the resolved config into franz-go client options. Kafka
// requires idempotence to be disabled for any acks level weaker than all, so the
// acks mapping and the idempotent toggle are handled together here.
func clientOpts(rc resolvedConfig) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(rc.brokers...)}

	switch rc.acks {
	case ackAll:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		if !rc.idempotent {
			opts = append(opts, kgo.DisableIdempotentWrite())
		}
	case ackLeader:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case ackNone:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	}

	if rc.txnID != "" {
		opts = append(opts, kgo.TransactionalID(rc.txnID))
	}

	// A configured group makes the client a group consumer from the start. For
	// direct (groupless) consumption the topic is added lazily in Prepare, once
	// the workload is known to contain consume operations.
	if rc.group != "" {
		opts = append(opts,
			kgo.ConsumerGroup(rc.group),
			kgo.ConsumeTopics(rc.topic.Name),
		)
	}
	return opts
}

// ensureTopic issues a CreateTopics request, tolerating a topic that already
// exists. Any other per-topic error code is surfaced.
func ensureTopic(ctx context.Context, cl *kgo.Client, tc topicConfig) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 15000
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic = tc.Name
	t.NumPartitions = tc.Partitions
	t.ReplicationFactor = tc.Replication
	req.Topics = append(req.Topics, t)

	raw, err := cl.Request(ctx, req)
	if err != nil {
		return fmt.Errorf("kafkadriver: create topic %q: %w", tc.Name, err)
	}
	resp, ok := raw.(*kmsg.CreateTopicsResponse)
	if !ok {
		return fmt.Errorf("kafkadriver: unexpected CreateTopics response %T", raw)
	}
	for _, rt := range resp.Topics {
		if rt.ErrorCode == 0 {
			continue
		}
		if kerr.ErrorForCode(rt.ErrorCode) == kerr.TopicAlreadyExists {
			continue // idempotent: an existing topic is fine
		}
		return fmt.Errorf("kafkadriver: create topic %q: %w", rt.Topic, kerr.ErrorForCode(rt.ErrorCode))
	}
	return nil
}

// errReadKeyUnsupported explains why per-node key reads are meaningless for a
// log-structured target.
var errReadKeyUnsupported = errors.New(
	"kafkadriver: ReadKeyFrom unsupported: Kafka is an append-only log with no per-key point read from a specific node; use the kafka-log verify model for correctness")
