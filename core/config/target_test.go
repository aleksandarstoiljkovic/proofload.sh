package config

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLoadTarget(t *testing.T) {
	tc, err := LoadTarget(filepath.Join("testdata", "target.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget: %v", err)
	}

	if tc.Target != "postgresql" {
		t.Errorf("Target = %q, want postgresql", tc.Target)
	}
	if tc.Image != "postgres" {
		t.Errorf("Image = %q, want postgres", tc.Image)
	}
	if tc.Version != "16" {
		t.Errorf("Version = %q, want 16", tc.Version)
	}

	wantDefaults := Defaults{Connections: 64, Warmup: 60 * time.Second, Duration: 300 * time.Second}
	if tc.Defaults != wantDefaults {
		t.Errorf("Defaults = %+v, want %+v", tc.Defaults, wantDefaults)
	}

	wantCluster := ClusterConfig{
		Backend:           "kubernetes",
		Nodes:             3,
		ReplicationFactor: 3,
		Consistency:       []string{"read-committed", "serializable"},
	}
	if !reflect.DeepEqual(tc.Cluster, wantCluster) {
		t.Errorf("Cluster = %+v, want %+v", tc.Cluster, wantCluster)
	}

	wantCaps := Capabilities{
		VerifyModels: []string{"reconciliation", "register"},
		Faults:       []string{"kill-node", "pause-node", "network-partition"},
	}
	if !reflect.DeepEqual(tc.Capabilities, wantCaps) {
		t.Errorf("Capabilities = %+v, want %+v", tc.Capabilities, wantCaps)
	}
}

func TestParseDurationInvalid(t *testing.T) {
	if _, err := parseDuration("notaduration", "warmup"); err == nil {
		t.Fatal("parseDuration(notaduration) = nil error, want error")
	}
	if d, err := parseDuration("", "warmup"); err != nil || d != 0 {
		t.Fatalf("parseDuration(\"\") = %v, %v; want 0, nil", d, err)
	}
}
