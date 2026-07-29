package report

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/emit"
)

// buildRun writes a synthetic run directory (manifest, summary, time-series and
// optionally a verify report) and returns its directory.
func buildRun(t *testing.T, withVerify bool, verdict domain.Verdict) string {
	t.Helper()
	base := t.TempDir()
	id := domain.RunID("run-1")
	p := emit.Layout(base, "postgres", id)
	if err := p.EnsureDir(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	m := domain.Manifest{
		RunID: id, Target: "postgres", Workload: "ycsb-a", Mode: domain.ModePerformance,
		Rate: domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: 5000}, Duration: 60 * time.Second,
		Connections: 32, Seed: 42, Trials: 1, EngineVersion: "v0",
		Cluster:   domain.ClusterSpec{ReplicationFactor: 3, Nodes: []domain.Node{{ID: "n1"}}},
		Client:    domain.ClientInfo{Hostname: "loadgen", OS: "linux", Arch: "arm64", CPUs: 8},
		Labels:    map[string]string{"env": "ci"},
		CreatedAt: now,
	}
	if err := emit.WriteManifest(p, m); err != nil {
		t.Fatal(err)
	}
	pct := func(base float64) domain.Percentiles {
		return domain.Percentiles{Count: 1000, Mean: base, P50: base, P90: base * 2, P95: base * 3,
			P99: base * 8, P999: base * 20, P9999: base * 40, Max: base * 60}
	}
	res := domain.RunResult{
		RunID: id, Phase: domain.PhaseMeasure, Total: 300000, Errors: 12, Duration: 60 * time.Second,
		Throughput: 1234.5, Overall: pct(0.5),
		ByOp: map[domain.OpType]domain.Percentiles{"read": pct(0.4), "write": pct(0.9)},
	}
	if err := emit.WriteSummary(p, res); err != nil {
		t.Fatal(err)
	}
	var snaps []domain.LatencySnapshot
	for i := 0; i < 5; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		snaps = append(snaps,
			domain.LatencySnapshot{T: ts, OpType: "read", Throughput: 800, Pct: pct(0.4)},
			domain.LatencySnapshot{T: ts, OpType: "write", Throughput: 400, Pct: pct(0.9)},
		)
	}
	if err := emit.WriteTimeseriesParquet(p, snaps); err != nil {
		t.Fatal(err)
	}
	if withVerify {
		v := domain.VerifyReport{
			Model: domain.VerifyReconciliation, Verdict: verdict, Checked: 300000,
			Lost: 0, Duplicated: 0, OrderingViol: 0, ConvergedIn: 2 * time.Second,
			Anomalies: []domain.Anomaly{{Kind: "lost-update", Detail: "key 7 lost", Witness: []string{"op-9"}}},
		}
		if err := emit.WriteVerify(p, v); err != nil {
			t.Fatal(err)
		}
	}
	return p.Dir
}

func TestRender(t *testing.T) {
	tests := []struct {
		name       string
		withVerify bool
		verdict    domain.Verdict
		want       []string
		absent     []string
	}{
		{
			name: "no verify", withVerify: false,
			want:   []string{"1234.5", "read", "write", "<svg", "ycsb-a", "postgres", "5,000", "300,000"},
			absent: []string{"Correctness"},
		},
		{
			name: "verify pass", withVerify: true, verdict: domain.VerdictPass,
			want: []string{"<svg", "Correctness", "PASS", "reconciliation", "lost-update"},
		},
		{
			name: "verify fail", withVerify: true, verdict: domain.VerdictFail,
			want: []string{"Correctness", "FAIL", "banner fail"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := buildRun(t, tc.withVerify, tc.verdict)
			out, err := Render(dir)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			html := string(out)
			if !strings.HasPrefix(html, "<!DOCTYPE html>") {
				t.Errorf("missing doctype")
			}
			for _, s := range tc.want {
				if !strings.Contains(html, s) {
					t.Errorf("html missing %q", s)
				}
			}
			for _, s := range tc.absent {
				if strings.Contains(html, s) {
					t.Errorf("html unexpectedly contains %q", s)
				}
			}
		})
	}
}

func TestRenderClientBoundBanner(t *testing.T) {
	base := t.TempDir()
	id := domain.RunID("cb")
	p := emit.Layout(base, "redis", id)
	if err := p.EnsureDir(); err != nil {
		t.Fatal(err)
	}
	if err := emit.WriteManifest(p, domain.Manifest{RunID: id, Target: "redis", Workload: "w", Mode: domain.ModePerformance}); err != nil {
		t.Fatal(err)
	}
	if err := emit.WriteSummary(p, domain.RunResult{RunID: id, Throughput: 10, ClientBound: true}); err != nil {
		t.Fatal(err)
	}
	out, err := Render(p.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "Client-bound") {
		t.Errorf("expected client-bound banner")
	}
}

func TestWriteReport(t *testing.T) {
	dir := buildRun(t, true, domain.VerdictPass)
	out, err := WriteReport(dir)
	if err != nil {
		t.Fatalf("WriteReport: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "<svg") {
		t.Errorf("written report missing svg")
	}
}
