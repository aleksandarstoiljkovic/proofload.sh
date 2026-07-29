package config

import (
	"path/filepath"
	"testing"

	"github.com/proofload/proofload/core/domain"
)

func TestLoadWorkloadMapsEveryField(t *testing.T) {
	w, err := LoadWorkload(filepath.Join("testdata", "workload.yaml"))
	if err != nil {
		t.Fatalf("LoadWorkload: %v", err)
	}

	if w.Name != "oltp_read_write" {
		t.Errorf("Name = %q, want oltp_read_write", w.Name)
	}
	if w.Mode != domain.ModePerformance {
		t.Errorf("Mode = %q, want %q", w.Mode, domain.ModePerformance)
	}
	if w.KeySpace != 10_000_000 {
		t.Errorf("KeySpace = %d, want 10000000", w.KeySpace)
	}
	if w.KeyDist != domain.DistZipfian {
		t.Errorf("KeyDist = %q, want %q", w.KeyDist, domain.DistZipfian)
	}
	if w.ValueSize != 100 {
		t.Errorf("ValueSize = %d, want 100", w.ValueSize)
	}
	if w.VerifyModel != domain.VerifyNone {
		t.Errorf("VerifyModel = %q, want empty", w.VerifyModel)
	}

	want := []domain.OpSpec{
		{Type: "read", Weight: 80},
		{Type: "update", Weight: 15},
		{Type: "insert", Weight: 5},
	}
	if len(w.Operations) != len(want) {
		t.Fatalf("Operations len = %d, want %d", len(w.Operations), len(want))
	}
	for i, op := range want {
		if w.Operations[i] != op {
			t.Errorf("Operations[%d] = %+v, want %+v", i, w.Operations[i], op)
		}
	}
}

func TestLoadWorkloadInvalidEnums(t *testing.T) {
	tests := []struct {
		name string
		file workloadFile
	}{
		{
			name: "bad mode",
			file: workloadFile{Mode: "nonsense", KeyDist: "zipfian"},
		},
		{
			name: "bad key_dist",
			file: workloadFile{Mode: "performance", KeyDist: "fibonacci"},
		},
		{
			name: "bad verify_model",
			file: workloadFile{Mode: "performance", KeyDist: "zipfian", VerifyModel: "quantum"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := mapWorkload(tt.file); err == nil {
				t.Fatalf("mapWorkload(%+v) = nil error, want error", tt.file)
			}
		})
	}
}

func TestLoadWorkloadInvalidFile(t *testing.T) {
	if _, err := LoadWorkload(filepath.Join("testdata", "workload_bad.yaml")); err == nil {
		t.Fatal("LoadWorkload(workload_bad.yaml) = nil error, want error")
	}
}
