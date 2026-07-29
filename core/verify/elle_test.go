package verify_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/verify"
)

// TestWriteElleEDN emits a small list-append history and asserts the EDN has
// the expected invoke/complete structure for both append and read mops.
func TestWriteElleEDN(t *testing.T) {
	h := writeHistory(t, []verify.Event{
		{Process: 0, F: "append", Key: 1, WVal: 100, Invoke: 10, Complete: 20, OK: true},
		{Process: 1, F: "read", Key: 1, RList: []int64{100, 200}, Invoke: 30, Complete: 40, OK: true},
	})
	path := filepath.Join(t.TempDir(), "history.edn")
	if err := verify.WriteElleEDN(h, path); err != nil {
		t.Fatalf("WriteElleEDN: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d EDN lines, want 4 (invoke+ok per event):\n%s", len(lines), b)
	}

	wants := []string{
		"{:type :invoke, :process 0, :value [[:append 1 100]]}",
		"{:type :ok, :process 0, :value [[:append 1 100]]}",
		"{:type :invoke, :process 1, :value [[:r 1 nil]]}",
		"{:type :ok, :process 1, :value [[:r 1 [100 200]]]}",
	}
	for i, want := range wants {
		if lines[i] != want {
			t.Errorf("line %d = %q, want %q", i, lines[i], want)
		}
	}
}

// TestWriteElleEDNFailedOp confirms a non-OK operation completes as :fail.
func TestWriteElleEDNFailedOp(t *testing.T) {
	h := writeHistory(t, []verify.Event{
		{Process: 2, F: "append", Key: 3, WVal: 42, Invoke: 10, Complete: 20, OK: false},
	})
	path := filepath.Join(t.TempDir(), "fail.edn")
	if err := verify.WriteElleEDN(h, path); err != nil {
		t.Fatalf("WriteElleEDN: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(b), "{:type :fail, :process 2,") {
		t.Errorf("expected a :fail completion, got:\n%s", b)
	}
}

// TestRunElleNoTooling asserts graceful degradation: with no elle-cli on PATH,
// RunElle returns VerdictUnknown and a "tooling" anomaly rather than failing.
// The real-exec path is skipped when the binary is present.
func TestRunElleNoTooling(t *testing.T) {
	if _, err := exec.LookPath("elle-cli"); err == nil {
		t.Skip("elle-cli present on PATH; skipping missing-tooling assertion")
	}
	report := verify.RunElle(context.Background(), filepath.Join(t.TempDir(), "history.edn"))

	if report.Model != domain.VerifyListAppend {
		t.Errorf("Model = %q, want %q", report.Model, domain.VerifyListAppend)
	}
	if report.Verdict != domain.VerdictUnknown {
		t.Errorf("Verdict = %q, want %q", report.Verdict, domain.VerdictUnknown)
	}
	if len(report.Anomalies) != 1 || report.Anomalies[0].Kind != "tooling" {
		t.Fatalf("anomalies = %+v, want one of kind \"tooling\"", report.Anomalies)
	}
	if report.Anomalies[0].Detail != "elle-cli not installed" {
		t.Errorf("detail = %q, want %q", report.Anomalies[0].Detail, "elle-cli not installed")
	}
}
