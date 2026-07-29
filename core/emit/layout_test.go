package emit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/proofload/proofload/core/domain"
)

func TestLayoutPaths(t *testing.T) {
	base := "/var/proofload"
	target := "postgres"
	id := domain.RunID("run-2026-07-28-abc")
	p := Layout(base, target, id)

	runDir := filepath.Join(base, "results", target, string(id))
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Dir", p.Dir, runDir},
		{"Manifest", p.Manifest, filepath.Join(runDir, "manifest.json")},
		{"Summary", p.Summary, filepath.Join(runDir, "summary.json")},
		{"TimeseriesNDJSON", p.TimeseriesNDJSON, filepath.Join(runDir, "timeseries.ndjson")},
		{"TimeseriesParquet", p.TimeseriesParquet, filepath.Join(runDir, "timeseries.parquet")},
		{"HLog", p.HLog, filepath.Join(runDir, "latency.hlog")},
		{"VerifyJSON", p.VerifyJSON, filepath.Join(runDir, "verify.json")},
		{"ServerStats", p.ServerStats, filepath.Join(runDir, "server_stats.json")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestEnsureDirCreatesRunDir(t *testing.T) {
	p := Layout(t.TempDir(), "redis", domain.RunID("r1"))
	if err := p.EnsureDir(); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	info, err := os.Stat(p.Dir)
	if err != nil {
		t.Fatalf("stat run dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("run path is not a directory")
	}
}
