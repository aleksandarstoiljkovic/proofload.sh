package verify_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/verify"
)

// TestReconcileConvergencePass uses a ClusterAware fake whose lagging replica
// catches up within the timeout.
func TestReconcileConvergencePass(t *testing.T) {
	value := []byte("converged")
	d := newClusterFake(value, []byte("stale"), 2)
	log := oneKeyLog(t, value)

	report, err := verify.Reconcile(context.Background(), d, driver.Config{}, log, verify.Options{
		ReadOp: readOp, ConvergenceSample: 1, ConvergenceTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Verdict != domain.VerdictPass {
		t.Fatalf("Verdict = %q, want pass (anomalies: %+v)", report.Verdict, report.Anomalies)
	}
	if report.ConvergedIn <= 0 || report.MaxStaleness <= 0 {
		t.Errorf("ConvergedIn=%v MaxStaleness=%v, want both > 0", report.ConvergedIn, report.MaxStaleness)
	}
}

// TestReconcileConvergenceFail uses a replica that never catches up before the
// timeout, so a divergence anomaly is raised.
func TestReconcileConvergenceFail(t *testing.T) {
	value := []byte("converged")
	d := newClusterFake(value, []byte("stale"), 1<<30) // effectively never converges
	log := oneKeyLog(t, value)

	report, err := verify.Reconcile(context.Background(), d, driver.Config{}, log, verify.Options{
		ReadOp: readOp, ConvergenceSample: 1, ConvergenceTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Verdict != domain.VerdictFail {
		t.Errorf("Verdict = %q, want fail", report.Verdict)
	}
	findAnomaly(t, report, "divergence")
}

func oneKeyLog(t *testing.T, value []byte) *verify.Log {
	t.Helper()
	path := filepath.Join(t.TempDir(), "expect.ndjson")
	writeRecords(t, path, []verify.WriteRecord{{Key: 1, Checksum: verify.Checksum(value), Seq: 1}})
	log, err := verify.ReadLog(path)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	return log
}
