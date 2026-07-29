package kafkadriver

import (
	"bytes"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecordFrom(t *testing.T) {
	op := domain.Operation{Type: "produce", Key: 42, Value: []byte("payload"), Seq: 7}
	rec := recordFrom("proofload", op)

	if rec.Topic != "proofload" {
		t.Errorf("topic = %q, want proofload", rec.Topic)
	}
	if got := string(rec.Key); got != "42" {
		t.Errorf("key = %q, want 42", got)
	}
	if !bytes.Equal(rec.Value, op.Value) {
		t.Errorf("value = %q, want %q", rec.Value, op.Value)
	}
	seq, err := seqFromHeaders(rec.Headers)
	if err != nil {
		t.Fatalf("seqFromHeaders: %v", err)
	}
	if seq != 7 {
		t.Errorf("seq header = %d, want 7", seq)
	}
}

func TestSeqRoundTrip(t *testing.T) {
	for _, seq := range []int64{0, 1, 255, 256, 1 << 20, 1<<63 - 1} {
		b := encodeSeq(seq)
		if len(b) != 8 {
			t.Fatalf("encodeSeq(%d) width = %d, want 8", seq, len(b))
		}
		got, err := decodeSeq(b)
		if err != nil {
			t.Fatalf("decodeSeq(%d): %v", seq, err)
		}
		if got != seq {
			t.Errorf("round-trip = %d, want %d", got, seq)
		}
	}
}

func TestSeqEncodingIsOrderPreserving(t *testing.T) {
	// Big-endian encoding must sort the same as the numeric values.
	a, b := encodeSeq(5), encodeSeq(6)
	if bytes.Compare(a, b) >= 0 {
		t.Errorf("encodeSeq(5) should sort before encodeSeq(6)")
	}
}

func TestDecodeSeqRejectsWrongWidth(t *testing.T) {
	for _, b := range [][]byte{nil, {1}, make([]byte, 7), make([]byte, 9)} {
		if _, err := decodeSeq(b); err == nil {
			t.Errorf("decodeSeq(len=%d): expected error", len(b))
		}
	}
}

func TestSeqFromHeadersMissing(t *testing.T) {
	if _, err := seqFromHeaders([]kgo.RecordHeader{{Key: "other", Value: []byte("x")}}); err == nil {
		t.Errorf("expected error when seq header absent")
	}
}

func TestKeyFromBytes(t *testing.T) {
	b := keyBytes(123456)
	got, err := keyFromBytes(b)
	if err != nil {
		t.Fatalf("keyFromBytes: %v", err)
	}
	if got != 123456 {
		t.Errorf("key round-trip = %d, want 123456", got)
	}
}

func TestBatchRecordAtDegeneratesToRecordFrom(t *testing.T) {
	// batch==1, i==0 must be byte-for-byte identical to the single-record path,
	// so enabling batching with batch_size:1 changes nothing.
	op := domain.Operation{Type: "produce_batch", Key: 42, Value: []byte("payload"), Seq: 7}
	single := recordFrom("proofload", op)
	batched := batchRecordAt("proofload", op, 0, 1)
	if !bytes.Equal(single.Key, batched.Key) {
		t.Errorf("key: batch==1 %q != single %q", batched.Key, single.Key)
	}
	seqS, _ := seqFromHeaders(single.Headers)
	seqB, _ := seqFromHeaders(batched.Headers)
	if seqS != seqB {
		t.Errorf("seq: batch==1 %d != single %d", seqB, seqS)
	}
}

func TestBatchRecordAtDistinctKeysNoCollision(t *testing.T) {
	// A batch fans out over `batch` distinct keys, and those keys never collide
	// with the keys of an adjacent op (op.Key and op.Key+1), so per-key sequence
	// streams stay clean for the verifier.
	const batch = 500
	seen := map[string]bool{}
	for _, opKey := range []int64{0, 1, 2, 1000} {
		op := domain.Operation{Key: opKey, Seq: 1}
		for i := 0; i < batch; i++ {
			k := string(batchRecordAt("t", op, i, batch).Key)
			if seen[k] {
				t.Fatalf("duplicate derived key %q (opKey=%d i=%d)", k, opKey, i)
			}
			seen[k] = true
		}
	}
	if len(seen) != 4*batch {
		t.Errorf("got %d distinct keys, want %d", len(seen), 4*batch)
	}
}

func TestProduceBatchRoleClassification(t *testing.T) {
	for _, tt := range []struct {
		t    domain.OpType
		want bool
	}{
		{"produce_batch", true}, {"wb", true},
		{"produce", false}, {"w", false}, {"consume", false},
	} {
		if got := isProduceBatch(tt.t); got != tt.want {
			t.Errorf("isProduceBatch(%q) = %v, want %v", tt.t, got, tt.want)
		}
	}
}

func TestRoleClassification(t *testing.T) {
	tests := []struct {
		t                domain.OpType
		produce, consume bool
	}{
		{"produce", true, false},
		{"w", true, false},
		{"consume", false, true},
		{"r", false, true},
		{"scan", false, false},
	}
	for _, tt := range tests {
		if isProduce(tt.t) != tt.produce {
			t.Errorf("isProduce(%q) = %v, want %v", tt.t, isProduce(tt.t), tt.produce)
		}
		if isConsume(tt.t) != tt.consume {
			t.Errorf("isConsume(%q) = %v, want %v", tt.t, isConsume(tt.t), tt.consume)
		}
	}
}
