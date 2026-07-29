package verify_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/testutil"
	"github.com/proofload/proofload/core/verify"
)

const readOp = domain.OpType("read")

// TestReconcilePass writes N keys through a FakeDriver, builds a matching log,
// and expects a clean pass.
func TestReconcilePass(t *testing.T) {
	d := testutil.NewFakeDriver()
	log := writeAndLog(t, d, 20)

	report, err := verify.Reconcile(context.Background(), d, driver.Config{}, log, verify.Options{ReadOp: readOp})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Model != domain.VerifyReconciliation {
		t.Errorf("Model = %q, want %q", report.Model, domain.VerifyReconciliation)
	}
	if report.Verdict != domain.VerdictPass {
		t.Errorf("Verdict = %q, want pass (anomalies: %+v)", report.Verdict, report.Anomalies)
	}
	if report.Checked != 20 || report.Lost != 0 || len(report.Anomalies) != 0 {
		t.Errorf("Checked=%d Lost=%d Anomalies=%d, want 20/0/0", report.Checked, report.Lost, len(report.Anomalies))
	}
}

// TestReconcileDetectsLoss expects an extra key never written to the store,
// simulating a lost committed write.
func TestReconcileDetectsLoss(t *testing.T) {
	d := testutil.NewFakeDriver()
	path := filepath.Join(t.TempDir(), "expect.ndjson")
	w := openWriter(t, path)
	writeKeys(t, d, w, 10)
	const lostKey = int64(9999)
	if err := w.Record(verify.WriteRecord{Key: lostKey, Checksum: verify.Checksum([]byte("gone")), Seq: 1}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	closeWriter(t, w)

	report := reconcile(t, d, path)
	if report.Verdict != domain.VerdictFail {
		t.Errorf("Verdict = %q, want fail", report.Verdict)
	}
	if report.Lost != 1 {
		t.Errorf("Lost = %d, want 1", report.Lost)
	}
	a := findAnomaly(t, report, "data-loss")
	if len(a.Witness) != 1 || a.Witness[0] != fmt.Sprint(lostKey) {
		t.Errorf("loss witness = %v, want [%d]", a.Witness, lostKey)
	}
}

// TestReconcileDetectsCorruption silently overwrites a key with an unrecorded
// value, so read-back disagrees with the logged checksum.
func TestReconcileDetectsCorruption(t *testing.T) {
	d := testutil.NewFakeDriver()
	path := filepath.Join(t.TempDir(), "expect.ndjson")
	w := openWriter(t, path)
	conn := connect(t, d)
	writeKeysVia(t, conn, w, 10)

	const badKey = int64(3)
	execWrite(t, conn, badKey, []byte("silently-mangled")) // not recorded in the log
	closeWriter(t, w)

	report := reconcile(t, d, path)
	if report.Verdict != domain.VerdictFail {
		t.Errorf("Verdict = %q, want fail", report.Verdict)
	}
	if report.Lost != 0 {
		t.Errorf("Lost = %d, want 0", report.Lost)
	}
	a := findAnomaly(t, report, "corruption")
	if len(a.Witness) != 1 || a.Witness[0] != fmt.Sprint(badKey) {
		t.Errorf("corruption witness = %v, want [%d]", a.Witness, badKey)
	}
}

// --- helpers ---

func writeAndLog(t *testing.T, d *testutil.FakeDriver, n int) *verify.Log {
	t.Helper()
	path := filepath.Join(t.TempDir(), "expect.ndjson")
	w := openWriter(t, path)
	writeKeys(t, d, w, n)
	closeWriter(t, w)
	log, err := verify.ReadLog(path)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	return log
}

func writeKeys(t *testing.T, d *testutil.FakeDriver, w *verify.LogWriter, n int) {
	t.Helper()
	writeKeysVia(t, connect(t, d), w, n)
}

func writeKeysVia(t *testing.T, conn driver.Conn, w *verify.LogWriter, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := int64(i)
		val := []byte(fmt.Sprintf("value-%d", i))
		execWrite(t, conn, key, val)
		if err := w.Record(verify.WriteRecord{Key: key, Checksum: verify.Checksum(val), Seq: 1}); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
}

func execWrite(t *testing.T, conn driver.Conn, key int64, val []byte) {
	t.Helper()
	res := conn.Execute(context.Background(), domain.Operation{Type: "write", Key: key, Value: val, Seq: 1})
	if res.Err != nil {
		t.Fatalf("write key %d: %v", key, res.Err)
	}
}

func connect(t *testing.T, d driver.Driver) driver.Conn {
	t.Helper()
	conn, err := d.Connect(context.Background(), driver.Config{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return conn
}

func openWriter(t *testing.T, path string) *verify.LogWriter {
	t.Helper()
	w, err := verify.NewLogWriter(path)
	if err != nil {
		t.Fatalf("NewLogWriter: %v", err)
	}
	return w
}

func closeWriter(t *testing.T, w *verify.LogWriter) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func reconcile(t *testing.T, d driver.Driver, path string) domain.VerifyReport {
	t.Helper()
	log, err := verify.ReadLog(path)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	report, err := verify.Reconcile(context.Background(), d, driver.Config{}, log, verify.Options{ReadOp: readOp})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return report
}

func findAnomaly(t *testing.T, report domain.VerifyReport, kind string) domain.Anomaly {
	t.Helper()
	for _, a := range report.Anomalies {
		if a.Kind == kind {
			return a
		}
	}
	t.Fatalf("no %q anomaly in %+v", kind, report.Anomalies)
	return domain.Anomaly{}
}
