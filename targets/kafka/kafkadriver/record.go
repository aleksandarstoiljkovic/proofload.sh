package kafkadriver

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/proofload/proofload/core/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

// record.go holds the pure functions that translate between a domain.Operation
// and a Kafka record: key/value construction and the per-key monotonic sequence
// number that is carried in a record header. Keeping these pure makes the
// produce/consume hot paths and the kafka-log verifier trivially unit testable
// without a broker.

// recordFrom builds the Kafka record for a produce operation. The key is the
// decimal form of op.Key so records for one logical key route to a single
// partition (preserving per-key order); the value is op.Value verbatim (the
// generator already embeds deterministic bytes); the per-key sequence travels in
// a header so the verifier can reconstruct order and detect loss/duplication.
func recordFrom(topic string, op domain.Operation) *kgo.Record {
	return &kgo.Record{
		Topic:   topic,
		Key:     keyBytes(op.Key),
		Value:   op.Value,
		Headers: []kgo.RecordHeader{{Key: seqHeaderKey, Value: encodeSeq(op.Seq)}},
	}
}

// batchRecordAt builds the i-th record of a batched produce (batch of size
// `batch`) for op. Each record gets a distinct, deterministic key derived from
// op.Key and the index — key = op.Key*batch + i — so the whole batch fans out
// across `batch` logical keys without ever colliding with the keys of an
// adjacent op (i is always < batch). The per-key sequence stays op.Seq: because
// the generator hands out a contiguous, per-key-monotonic Seq (1,2,3,…) for each
// op.Key it draws, every derived key likewise observes a contiguous 1,2,3,…
// stream, so the kafka-log Verifier sees no false gaps or duplicates. With
// batch == 1 this is byte-for-byte identical to recordFrom (key op.Key, seq
// op.Seq), so the single-record produce path is just the batch==1 special case.
func batchRecordAt(topic string, op domain.Operation, i, batch int) *kgo.Record {
	key := op.Key*int64(batch) + int64(i)
	return &kgo.Record{
		Topic:   topic,
		Key:     keyBytes(key),
		Value:   op.Value,
		Headers: []kgo.RecordHeader{{Key: seqHeaderKey, Value: encodeSeq(op.Seq)}},
	}
}

// keyBytes renders a key as its decimal ASCII form.
func keyBytes(key int64) []byte {
	return []byte(strconv.FormatInt(key, 10))
}

// keyFromBytes parses a key produced by keyBytes.
func keyFromBytes(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

// encodeSeq encodes a sequence number as 8 big-endian bytes so byte-wise order
// matches numeric order and decoding is unambiguous.
func encodeSeq(seq int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq))
	return b[:]
}

// decodeSeq reverses encodeSeq, rejecting any header value of the wrong width.
func decodeSeq(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("kafkadriver: seq header has %d bytes, want 8", len(b))
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// seqFromHeaders extracts the sequence number from a record's headers.
func seqFromHeaders(hs []kgo.RecordHeader) (int64, error) {
	for _, h := range hs {
		if h.Key == seqHeaderKey {
			return decodeSeq(h.Value)
		}
	}
	return 0, fmt.Errorf("kafkadriver: record missing %q header", seqHeaderKey)
}

// isProduce/isConsume classify an operation type into the two roles this target
// supports. Both a verbose and a single-letter alias are accepted so the same
// workload op labels used by other targets ("w"/"r") work here too.
func isProduce(t domain.OpType) bool { return t == "produce" || t == "w" }
func isConsume(t domain.OpType) bool { return t == "consume" || t == "r" }

// isProduceBatch classifies the batched-produce role. A single Execute of this
// op type emits params.batch_size records in one ProduceSync call so throughput
// reflects Kafka's real batched ingest rather than the ~one-record-per-RTT rate
// of a synchronous single produce. It shares the produce hot path (see
// kafkaConn.produce): the only difference from "produce" is the record count,
// which is driven by the resolved batchSize.
func isProduceBatch(t domain.OpType) bool { return t == "produce_batch" || t == "wb" }
