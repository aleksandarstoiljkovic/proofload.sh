package warehouse

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/emit"
)

// writeRun emits a synthetic run under base and returns its directory.
func writeRun(t *testing.T, base, target, id string, created time.Time, tput, p99 float64, withVerify bool) string {
	t.Helper()
	p := emit.Layout(base, target, domain.RunID(id))
	if err := p.EnsureDir(); err != nil {
		t.Fatal(err)
	}
	m := domain.Manifest{
		RunID: domain.RunID(id), Target: target, Workload: "ycsb-a",
		Mode: domain.ModePerformance, CreatedAt: created,
	}
	if err := emit.WriteManifest(p, m); err != nil {
		t.Fatal(err)
	}
	res := domain.RunResult{
		RunID: domain.RunID(id), Phase: domain.PhaseMeasure, Total: 1000, Errors: 3,
		Throughput: tput, Overall: domain.Percentiles{P50: 1, P99: p99, P999: p99 * 2, Max: p99 * 4},
	}
	if err := emit.WriteSummary(p, res); err != nil {
		t.Fatal(err)
	}
	if withVerify {
		v := domain.VerifyReport{Model: domain.VerifyReconciliation, Verdict: domain.VerdictPass}
		if err := emit.WriteVerify(p, v); err != nil {
			t.Fatal(err)
		}
	}
	return p.Dir
}

func TestIngestAndRuns(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "wh.duckdb")
	w, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	older := writeRun(t, base, "postgres", "run-old", now.Add(-time.Hour), 900, 12.5, false)
	newer := writeRun(t, base, "postgres", "run-new", now, 1500, 7.25, true)

	if err := w.Ingest(older); err != nil {
		t.Fatalf("ingest older: %v", err)
	}
	if err := w.Ingest(newer); err != nil {
		t.Fatalf("ingest newer: %v", err)
	}

	runs, err := w.Runs("postgres", "ycsb-a", 10)
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("want 2 runs, got %d", len(runs))
	}
	// Most recent first.
	if runs[0].RunID != "run-new" || runs[1].RunID != "run-old" {
		t.Errorf("wrong order: %s then %s", runs[0].RunID, runs[1].RunID)
	}
	if runs[0].Throughput != 1500 || runs[0].P99 != 7.25 {
		t.Errorf("run-new fields wrong: tput=%v p99=%v", runs[0].Throughput, runs[0].P99)
	}
	if runs[1].P99 != 12.5 {
		t.Errorf("run-old p99 wrong: %v", runs[1].P99)
	}
	if runs[0].VerifyVerdict != string(domain.VerdictPass) {
		t.Errorf("run-new verdict = %q, want pass", runs[0].VerifyVerdict)
	}
	if runs[1].VerifyVerdict != "" {
		t.Errorf("run-old verdict = %q, want empty", runs[1].VerifyVerdict)
	}
}

func TestIngestIdempotent(t *testing.T) {
	base := t.TempDir()
	w, err := Open(filepath.Join(t.TempDir(), "wh.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	dir := writeRun(t, base, "redis", "run-1", now, 1000, 5, false)
	if err := w.Ingest(dir); err != nil {
		t.Fatal(err)
	}
	// Re-emit the same run with a changed throughput, then re-ingest.
	dir = writeRun(t, base, "redis", "run-1", now, 2000, 9, false)
	if err := w.Ingest(dir); err != nil {
		t.Fatal(err)
	}

	runs, err := w.Runs("", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("re-ingest duplicated: got %d rows", len(runs))
	}
	if runs[0].Throughput != 2000 {
		t.Errorf("upsert did not update: throughput=%v", runs[0].Throughput)
	}
}
