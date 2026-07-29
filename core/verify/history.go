package verify

import (
	"strings"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/runner"
)

// Event is one recorded MEASURE-phase operation in a correctness history — the
// on-disk (NDJSON) and in-memory unit consumed by the register (Porcupine) and
// list-append (Elle) checkers.
//
// F is the operation family: "read", "write", or "append". WVal is the token
// written by a write/append (this package's Checksum of the value bytes). RVal
// is the Checksum observed by a register read (0 when nothing was observed).
// RList is the list observed by a list-append read (or read back by an append).
// [Invoke, Complete] are wall-clock unix nanoseconds forming a closed real-time
// interval.
//
// VALUE-DISTINGUISHABILITY REQUIREMENT: register linearizability and list-append
// isolation are only MEANINGFUL when each write carries a DISTINGUISHABLE value,
// so a read attributes to the specific write it observed. proofload's current
// generator emits a deterministic Value(key,size) — identical bytes per key — so
// every write to a key hashes to the SAME WVal and live histories are not yet
// distinguishable. The checkers here are therefore correct and unit-proven on
// synthetic histories; meaningful LIVE results need a future versioned/append
// workload whose writes carry unique tokens.
type Event struct {
	Process  int     `json:"p"`
	F        string  `json:"f"`
	Key      int64   `json:"k"`
	WVal     uint64  `json:"wv,omitempty"`
	RVal     uint64  `json:"rv,omitempty"`
	RList    []int64 `json:"rl,omitempty"`
	Invoke   int64   `json:"inv"`
	Complete int64   `json:"cmp"`
	OK       bool    `json:"ok"`
}

// Operation families.
const (
	fRead   = "read"
	fWrite  = "write"
	fAppend = "append"
)

// HistoryRecorder implements runner.OpObserver for one process (shard),
// classifying each observed operation and appending an Event to a shared
// HistoryWriter. Observe is called from a SINGLE goroutine (one recorder per
// connection), so the recorder needs no locking; the HistoryWriter serializes
// writes across recorders.
type HistoryRecorder struct {
	process int
	w       *HistoryWriter
}

var _ runner.OpObserver = (*HistoryRecorder)(nil)

// NewHistoryRecorder builds a recorder tagging every Event with process and
// appending to w. All per-shard recorders may share one HistoryWriter.
func NewHistoryRecorder(process int, w *HistoryWriter) *HistoryRecorder {
	return &HistoryRecorder{process: process, w: w}
}

// Observe classifies op into a read/write/append Event and records it: an
// "append"-typed op is an append, an op with no value bytes is a read, anything
// else is a write. Read output comes from res.Observed (a scalar checksum for
// registers, a list for list-append).
func (r *HistoryRecorder) Observe(op domain.Operation, res domain.OpResult, invoke, complete time.Time) {
	e := Event{
		Process:  r.process,
		Key:      op.Key,
		Invoke:   invoke.UnixNano(),
		Complete: complete.UnixNano(),
		OK:       res.Err == nil,
	}
	switch {
	case isAppendOp(op.Type):
		e.F = fAppend
		e.WVal = Checksum(op.Value)
		if list, ok := observedList(res.Observed); ok {
			e.RList = list
		}
	case len(op.Value) == 0:
		e.F = fRead
		if list, ok := observedList(res.Observed); ok {
			e.RList = list
		} else if b, present := coerceBytes(res.Observed); present {
			e.RVal = Checksum(b)
		}
	default:
		e.F = fWrite
		e.WVal = Checksum(op.Value)
	}
	_ = r.w.Append(e)
}

// isAppendOp reports whether a target-defined op type denotes a list append.
func isAppendOp(t domain.OpType) bool {
	return strings.Contains(strings.ToLower(string(t)), "append")
}

// observedList coerces a driver's Observed into a token list for list-append
// reads, accepting common slice shapes. A non-slice observation (scalar
// register value, nil) yields ok=false.
func observedList(v any) (list []int64, ok bool) {
	switch x := v.(type) {
	case []int64:
		return append([]int64(nil), x...), true
	case []uint64:
		return toInt64s(x), true
	case []int:
		return toInt64s(x), true
	case [][]byte:
		out := make([]int64, len(x))
		for i, e := range x {
			out[i] = int64(Checksum(e))
		}
		return out, true
	default:
		return nil, false
	}
}

// toInt64s converts a slice of any integer token type to []int64.
func toInt64s[T ~int | ~uint64](s []T) []int64 {
	out := make([]int64, len(s))
	for i, e := range s {
		out[i] = int64(e)
	}
	return out
}
