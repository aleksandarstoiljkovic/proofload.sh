package emit

import (
	"reflect"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

func sampleManifest() domain.Manifest {
	return domain.Manifest{
		RunID:         "run-1",
		Target:        "postgres",
		TargetVersion: "16.2",
		Workload:      "ycsb-a",
		Mode:          domain.ModePerformance,
		Rate:          domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: 5000},
		Duration:      5 * time.Minute,
		Warmup:        30 * time.Second,
		Connections:   64,
		Seed:          42,
		Trials:        3,
		Consistency:   "linearizable",
		Cluster: domain.ClusterSpec{
			Nodes:             []domain.Node{{ID: "n1"}},
			ReplicationFactor: 3,
			Consistency:       []string{"quorum"},
		},
		Faults: []domain.FaultSpec{{
			Fault:    domain.Fault{Type: domain.FaultKillNode, Target: "n1"},
			At:       10 * time.Second,
			Duration: 5 * time.Second,
		}},
		EngineVersion: "0.1.0",
		GitSHA:        "deadbeef",
		Client:        domain.ClientInfo{Hostname: "load-1", OS: "linux", Arch: "arm64", CPUs: 8},
		Labels:        map[string]string{"env": "ci"},
		CreatedAt:     time.Unix(1_700_000_000, 0).UTC(),
	}
}

func TestManifestRoundTrip(t *testing.T) {
	p := Layout(t.TempDir(), "postgres", "run-1")
	want := sampleManifest()

	if err := WriteManifest(p, want); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	got, err := ReadManifest(p)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestSummaryRoundTrip(t *testing.T) {
	p := Layout(t.TempDir(), "postgres", "run-1")
	want := domain.RunResult{
		RunID:      "run-1",
		Phase:      domain.PhaseMeasure,
		Total:      1_000_000,
		Errors:     12,
		Duration:   5 * time.Minute,
		Throughput: 3333.3,
		Overall:    domain.Percentiles{Count: 1_000_000, Mean: 1.2, P50: 1.0, P99: 9.9, Max: 42.0},
		ByOp: map[domain.OpType]domain.Percentiles{
			"read":  {Count: 800_000, P50: 0.8, P99: 5.0},
			"write": {Count: 200_000, P50: 2.0, P99: 15.0},
		},
		ClientBound: true,
	}

	if err := WriteSummary(p, want); err != nil {
		t.Fatalf("WriteSummary: %v", err)
	}
	got, err := ReadSummary(p)
	if err != nil {
		t.Fatalf("ReadSummary: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestVerifyRoundTrip(t *testing.T) {
	p := Layout(t.TempDir(), "postgres", "run-1")
	want := domain.VerifyReport{
		Model:        domain.VerifyListAppend,
		Verdict:      domain.VerdictFail,
		Anomalies:    []domain.Anomaly{{Kind: "G2-item", Detail: "cycle", Witness: []string{"t1", "t2"}}},
		Checked:      5000,
		Lost:         1,
		Duplicated:   2,
		OrderingViol: 3,
		ConvergedIn:  2 * time.Second,
		MaxStaleness: 500 * time.Millisecond,
		Extra:        map[string]any{"note": "flaky"},
	}

	if err := WriteVerify(p, want); err != nil {
		t.Fatalf("WriteVerify: %v", err)
	}
	got, err := ReadVerify(p)
	if err != nil {
		t.Fatalf("ReadVerify: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}
