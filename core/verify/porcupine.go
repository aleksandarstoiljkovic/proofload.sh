package verify

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/proofload/proofload/core/domain"
)

// checkTimeout bounds each per-key linearizability check. Linearizability is
// NP-hard; a bound keeps a pathological history from hanging a run. A key that
// exceeds it is reported Unknown rather than Pass or Fail.
const checkTimeout = 5 * time.Second

// witnessCap limits how many operations a non-linearizable anomaly enumerates,
// so a huge broken key does not produce an unbounded witness.
const witnessCap = 32

// regInput is the input to the register model: a write (put) of value, or a
// read (get). key lets CheckRegister group operations without a partition
// function — each key is checked as an independent register.
type regInput struct {
	write bool
	key   int64
	value uint64
}

// registerModel returns a Porcupine model of a single read/write register whose
// state is the last-written token (0 = never written / empty). A write always
// succeeds and sets the value; a read succeeds only if it observes the current
// value, which is what makes stale or out-of-real-time reads illegal.
func registerModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return uint64(0) },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(uint64)
			in := input.(regInput)
			if in.write {
				return true, in.value
			}
			return output.(uint64) == st, st
		},
		Equal: func(a, b interface{}) bool { return a.(uint64) == b.(uint64) },
	}
}

// CheckRegister models each key as an independent read/write register and runs
// Porcupine per key. A key whose history is not linearizable yields a
// "non-linearizable" anomaly with a witness op trace; any such key fails the
// verdict. A key whose check times out is reported Unknown (never a false Fail).
// Reads of never-written keys observe 0, matching the model's initial state, so
// they are handled gracefully. Checked is set to the number of keys examined.
func CheckRegister(h *History) domain.VerifyReport {
	report := domain.VerifyReport{Model: domain.VerifyRegister, Verdict: domain.VerdictPass}
	keys := h.Keys()
	report.Checked = int64(len(keys))
	model := registerModel()

	sawIllegal, sawUnknown := false, false
	for _, key := range keys {
		ops := registerOps(h.ByKey(key))
		if len(ops) == 0 {
			continue
		}
		res, _ := porcupine.CheckOperationsVerbose(model, ops, checkTimeout)
		switch res {
		case porcupine.Illegal:
			sawIllegal = true
			report.OrderingViol++
			report.Anomalies = append(report.Anomalies, domain.Anomaly{
				Kind:    "non-linearizable",
				Detail:  fmt.Sprintf("key %d register history is not linearizable", key),
				Witness: registerWitness(key, h.ByKey(key)),
			})
		case porcupine.Unknown:
			sawUnknown = true
			report.Anomalies = append(report.Anomalies, domain.Anomaly{
				Kind:    "unknown",
				Detail:  fmt.Sprintf("key %d linearizability check timed out after %s", key, checkTimeout),
				Witness: []string{fmt.Sprintf("key=%d", key)},
			})
		}
	}

	switch {
	case sawIllegal:
		report.Verdict = domain.VerdictFail
	case sawUnknown:
		report.Verdict = domain.VerdictUnknown
	}
	return report
}

// registerOps turns one key's Events into Porcupine operations. Writes become
// put inputs; reads become get inputs whose output is the observed token.
// Appends are ignored (they belong to the list-append model, not a register).
// Invoke/Complete are used as the closed real-time interval [Call, Return].
func registerOps(events []Event) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, len(events))
	for _, e := range events {
		switch e.F {
		case fWrite:
			ops = append(ops, porcupine.Operation{
				ClientId: e.Process,
				Input:    regInput{write: true, key: e.Key, value: e.WVal},
				Output:   uint64(0),
				Call:     e.Invoke,
				Return:   e.Complete,
			})
		case fRead:
			ops = append(ops, porcupine.Operation{
				ClientId: e.Process,
				Input:    regInput{write: false, key: e.Key},
				Output:   e.RVal,
				Call:     e.Invoke,
				Return:   e.Complete,
			})
		}
	}
	return ops
}

// registerWitness renders a compact, reproducible trace of the operations on a
// broken key: each entry is "p<proc>:<w|r><token>@[invoke,complete]".
func registerWitness(key int64, events []Event) []string {
	out := []string{fmt.Sprintf("key=%d", key)}
	for _, e := range events {
		if len(out) > witnessCap {
			out = append(out, "...")
			break
		}
		switch e.F {
		case fWrite:
			out = append(out, fmt.Sprintf("p%d:w%d@[%d,%d]", e.Process, e.WVal, e.Invoke, e.Complete))
		case fRead:
			out = append(out, fmt.Sprintf("p%d:r%d@[%d,%d]", e.Process, e.RVal, e.Invoke, e.Complete))
		}
	}
	return out
}
