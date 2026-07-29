package kafkadriver

import (
	"context"
	"fmt"

	"github.com/proofload/proofload/core/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaConn is a single kgo.Client used by one runner goroutine. The client
// plays two roles: a synchronous producer (produce/w) and, when the workload
// consumes, a poller (consume/r).
type kafkaConn struct {
	cl            *kgo.Client
	topic         string
	transactional bool // wrap each produce in a committed transaction (EOS)
	grouped       bool // client already joined a consumer group at Connect
	batchSize     int  // records produced per produce Execute (>=1)
}

// Prepare inspects the workload and, for a groupless client that will consume,
// starts direct consumption of the topic. Group consumers were already wired to
// the topic at Connect. Producing needs no preparation.
func (c *kafkaConn) Prepare(_ context.Context, w domain.Workload) error {
	if !c.grouped && workloadConsumes(w) {
		c.cl.AddConsumeTopics(c.topic)
	}
	return nil
}

// Execute runs exactly one operation. It never times itself; errors are
// returned inside the OpResult so the runner can bucket them per op type.
func (c *kafkaConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	switch {
	case isProduce(op.Type), isProduceBatch(op.Type):
		return c.produce(ctx, op)
	case isConsume(op.Type):
		return c.consume(ctx, op)
	default:
		return domain.OpResult{Type: op.Type, Err: fmt.Errorf("kafkadriver: unsupported op type %q", op.Type)}
	}
}

// produce synchronously produces a batch of batchSize records in a single
// ProduceSync call so the runner measures the real end-of-ack latency for the
// whole batch. franz-go coalesces the records into produce requests, so a large
// batch amortises the broker round-trip and throughput reaches Kafka's true
// batched ingest (throughput in ops/s × batchSize = msgs/s). batchSize == 1 is
// the classic single-record produce (one record, keyed op.Key, seq op.Seq).
// When transactional, the whole batch is wrapped in one committed transaction to
// exercise the EOS commit path end to end. Rows is set to the record count so
// the run summary reflects messages, not batches.
func (c *kafkaConn) produce(ctx context.Context, op domain.Operation) domain.OpResult {
	res := domain.OpResult{Type: op.Type}
	n := c.batchSize
	if n < 1 {
		n = 1
	}
	recs := make([]*kgo.Record, n)
	if n == 1 {
		recs[0] = recordFrom(c.topic, op)
	} else {
		for i := 0; i < n; i++ {
			recs[i] = batchRecordAt(c.topic, op, i, n)
		}
	}

	if c.transactional {
		if err := c.cl.BeginTransaction(); err != nil {
			res.Err = fmt.Errorf("kafkadriver: begin txn: %w", err)
			return res
		}
	}

	if err := c.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		if c.transactional {
			_ = c.cl.EndTransaction(ctx, kgo.TryAbort)
		}
		res.Err = err
		return res
	}

	if c.transactional {
		if err := c.cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			res.Err = fmt.Errorf("kafkadriver: commit txn: %w", err)
			return res
		}
	}
	res.Rows = n
	return res
}

// consume polls exactly one record and returns its value in Observed. An empty
// poll (no record currently available) is not an error: Observed stays nil and
// Rows is 0, mirroring how the reference target treats a missing key.
func (c *kafkaConn) consume(ctx context.Context, op domain.Operation) domain.OpResult {
	res := domain.OpResult{Type: op.Type}
	fetches := c.cl.PollRecords(ctx, 1)
	if err := fetches.Err(); err != nil {
		res.Err = err
		return res
	}
	recs := fetches.Records()
	if len(recs) == 0 {
		return res
	}
	res.Observed = recs[0].Value
	res.Rows = 1
	return res
}

// Close flushes and releases the client.
func (c *kafkaConn) Close() error {
	if c.cl == nil {
		return nil
	}
	c.cl.Close()
	return nil
}

// workloadConsumes reports whether any operation in the mix is a consume role.
func workloadConsumes(w domain.Workload) bool {
	for _, op := range w.Operations {
		if isConsume(op.Type) {
			return true
		}
	}
	return false
}
