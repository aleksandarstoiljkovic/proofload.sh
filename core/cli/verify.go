package cli

import (
	"context"
	"os"
	"time"

	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/emit"
	"github.com/proofload/proofload/core/runner"
	"github.com/proofload/proofload/core/verify"
)

// resolveVerifyModel picks the verification model: the --verify flag wins,
// otherwise the workload's declared verify_model (empty = no verification).
func resolveVerifyModel(f *runFlags, w domain.Workload) domain.VerifyModel {
	if f.verify != "" {
		return domain.VerifyModel(f.verify)
	}
	return w.VerifyModel
}

// verification carries the state needed to record and check correctness for one
// run. It is a no-op when model is empty. Reconciliation uses an expectation
// log; register/list-append use a full operation history.
type verification struct {
	model  domain.VerifyModel
	path   string                // temp file (expectation log or history)
	expLog *verify.LogWriter     // set for reconciliation
	histW  *verify.HistoryWriter // set for register/list-append
	readOp domain.OpType
}

// newVerification prepares recording for the requested model: an expectation log
// (reconciliation) or an operation history (register/list-append).
func newVerification(model domain.VerifyModel, r config.Resolved) (*verification, error) {
	v := &verification{model: model}
	switch model {
	case domain.VerifyReconciliation:
		p, err := tempFile("proofload-expect-*.ndjson")
		if err != nil {
			return nil, err
		}
		v.path = p
		lw, err := verify.NewLogWriter(p)
		if err != nil {
			return nil, err
		}
		v.expLog = lw
		v.readOp = pickReadOp(r.Workload)
	case domain.VerifyRegister, domain.VerifyListAppend:
		p, err := tempFile("proofload-history-*.ndjson")
		if err != nil {
			return nil, err
		}
		v.path = p
		hw, err := verify.NewHistoryWriter(p)
		if err != nil {
			return nil, err
		}
		v.histW = hw
	}
	return v, nil
}

func tempFile(pattern string) (string, error) {
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	return name, tmp.Close()
}

// observer returns the per-connection runner observer that records correctness
// data (committed writes for reconciliation, or a full per-process history for
// register/list-append), or nil when no recording is active.
func (v *verification) observer() func(shard int) runner.OpObserver {
	if v.expLog != nil {
		obs := writeLogObserver{w: v.expLog}
		return func(shard int) runner.OpObserver { return obs }
	}
	if v.histW != nil {
		return func(shard int) runner.OpObserver { return verify.NewHistoryRecorder(shard, v.histW) }
	}
	return nil
}

// run executes the verification after the load phase and returns its report, or
// nil when no model was selected.
func (v *verification) run(ctx context.Context, e Engine, r config.Resolved, p emit.Paths) (*domain.VerifyReport, error) {
	if v.model == domain.VerifyNone {
		return nil, nil
	}
	// Prefer a driver-native verifier (e.g. Kafka's kafka-log checker).
	if vr, ok := e.Driver.(driver.Verifier); ok && vr.Model() == v.model {
		rep, err := vr.Verify(ctx, driver.RunArtifacts{Dir: p.Dir, Cfg: r.Driver})
		return &rep, err
	}
	switch v.model {
	case domain.VerifyReconciliation:
		return v.reconcile(ctx, e, r)
	case domain.VerifyRegister:
		return v.checkRegister()
	case domain.VerifyListAppend:
		return v.checkListAppend(ctx)
	}
	rep := domain.VerifyReport{
		Model:   v.model,
		Verdict: domain.VerdictUnknown,
		Anomalies: []domain.Anomaly{{
			Kind:   "unsupported",
			Detail: "no checker available for model " + string(v.model) + " on target " + e.Name,
		}},
	}
	return &rep, nil
}

// checkRegister runs the Porcupine per-key linearizability checker on the
// recorded history.
func (v *verification) checkRegister() (*domain.VerifyReport, error) {
	if err := v.histW.Close(); err != nil {
		return nil, err
	}
	h, err := verify.ReadHistory(v.path)
	if err != nil {
		return nil, err
	}
	rep := verify.CheckRegister(h)
	return &rep, nil
}

// checkListAppend emits the Elle EDN history and runs the external elle-cli
// isolation checker (gracefully unknown when elle-cli is not installed).
func (v *verification) checkListAppend(ctx context.Context) (*domain.VerifyReport, error) {
	if err := v.histW.Close(); err != nil {
		return nil, err
	}
	h, err := verify.ReadHistory(v.path)
	if err != nil {
		return nil, err
	}
	edn := v.path + ".edn"
	if err := verify.WriteElleEDN(h, edn); err != nil {
		return nil, err
	}
	rep := verify.RunElle(ctx, edn)
	return &rep, nil
}

func (v *verification) reconcile(ctx context.Context, e Engine, r config.Resolved) (*domain.VerifyReport, error) {
	if err := v.expLog.Close(); err != nil {
		return nil, err
	}
	log, err := verify.ReadLog(v.path)
	if err != nil {
		return nil, err
	}
	rep, err := verify.Reconcile(ctx, e.Driver, r.Driver, log, verify.Options{
		ReadOp:             v.readOp,
		Workload:           r.Workload,
		ConvergenceSample:  200,
		ConvergenceTimeout: 5 * time.Second,
	})
	return &rep, err
}

// cleanup closes and removes the temporary recording file(s).
func (v *verification) cleanup() {
	if v.expLog != nil {
		_ = v.expLog.Close()
	}
	if v.histW != nil {
		_ = v.histW.Close()
	}
	if v.path != "" {
		_ = os.Remove(v.path)
		_ = os.Remove(v.path + ".edn")
	}
}

// pickReadOp chooses the read operation type the reconciliation checker uses to
// read values back, based on the workload's operation set.
func pickReadOp(w domain.Workload) domain.OpType {
	present := map[domain.OpType]bool{}
	for _, op := range w.Operations {
		present[op.Type] = true
	}
	for _, cand := range []domain.OpType{"read", "r", "get"} {
		if present[cand] {
			return cand
		}
	}
	return "read"
}

// writeLogObserver records every committed write (an op that carries a value and
// did not error) into the reconciliation expectation log. verify.LogWriter is
// safe for concurrent Record calls, so one instance is shared across shards.
type writeLogObserver struct{ w *verify.LogWriter }

func (o writeLogObserver) Observe(op domain.Operation, res domain.OpResult, _, _ time.Time) {
	if res.Err != nil || len(op.Value) == 0 {
		return
	}
	_ = o.w.Record(verify.WriteRecord{
		Key:      op.Key,
		Checksum: verify.Checksum(op.Value),
		Seq:      op.Seq,
	})
}
