package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

func baseOptions() ResolveOptions {
	return ResolveOptions{
		GlobalPath:   filepath.Join("testdata", "global.yaml"),
		TargetPath:   filepath.Join("testdata", "target.yaml"),
		WorkloadPath: filepath.Join("testdata", "workload.yaml"),
	}
}

func TestResolvePrecedence(t *testing.T) {
	// global sets connections=16; target defaults override it to 64.
	got, err := Resolve(baseOptions())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Manifest.Connections != 64 {
		t.Errorf("target should override global: Connections = %d, want 64", got.Manifest.Connections)
	}

	// env overrides target defaults.
	t.Setenv("PROOFLOAD_CONNECTIONS", "128")
	got, err = Resolve(baseOptions())
	if err != nil {
		t.Fatalf("Resolve with env: %v", err)
	}
	if got.Manifest.Connections != 128 {
		t.Errorf("env should override target: Connections = %d, want 128", got.Manifest.Connections)
	}

	// explicit Overrides beat env.
	o := baseOptions()
	o.Overrides = map[string]any{"connections": 256}
	got, err = Resolve(o)
	if err != nil {
		t.Fatalf("Resolve with overrides: %v", err)
	}
	if got.Manifest.Connections != 256 {
		t.Errorf("overrides should beat env: Connections = %d, want 256", got.Manifest.Connections)
	}
}

func TestResolveDurationsAndEnvLists(t *testing.T) {
	t.Setenv("PROOFLOAD_DURATION", "120s")
	t.Setenv("PROOFLOAD_WARMUP", "5m")
	t.Setenv("PROOFLOAD_ENDPOINTS", "a:5432,b:5432,c:5432")
	t.Setenv("PROOFLOAD_CONSISTENCY", "serializable")

	got, err := Resolve(baseOptions())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Manifest.Duration != 120*time.Second {
		t.Errorf("Duration = %v, want 120s", got.Manifest.Duration)
	}
	if got.Manifest.Warmup != 5*time.Minute {
		t.Errorf("Warmup = %v, want 5m", got.Manifest.Warmup)
	}
	if got.Manifest.Consistency != "serializable" {
		t.Errorf("Consistency = %q, want serializable", got.Manifest.Consistency)
	}
	wantEndpoints := []string{"a:5432", "b:5432", "c:5432"}
	if len(got.Driver.Endpoints) != len(wantEndpoints) {
		t.Fatalf("Endpoints = %v, want %v", got.Driver.Endpoints, wantEndpoints)
	}
	for i, e := range wantEndpoints {
		if got.Driver.Endpoints[i] != e {
			t.Errorf("Endpoints[%d] = %q, want %q", i, got.Driver.Endpoints[i], e)
		}
	}
}

func TestResolveMapsClusterBackendToTopology(t *testing.T) {
	got, err := Resolve(baseOptions())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Topology.Backend != domain.BackendKubernetes {
		t.Errorf("Topology.Backend = %q, want %q", got.Topology.Backend, domain.BackendKubernetes)
	}
	if got.Topology.Nodes != 3 {
		t.Errorf("Topology.Nodes = %d, want 3", got.Topology.Nodes)
	}
	if got.Topology.ReplicationFactor != 3 {
		t.Errorf("Topology.ReplicationFactor = %d, want 3", got.Topology.ReplicationFactor)
	}
	if got.Topology.Image != "postgres" || got.Topology.Version != "16" {
		t.Errorf("Topology image/version = %q/%q, want postgres/16", got.Topology.Image, got.Topology.Version)
	}
}

func TestResolveConsistencyFallsBackToClusterList(t *testing.T) {
	got, err := Resolve(baseOptions())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Driver.Consistency != "read-committed" {
		t.Errorf("Consistency = %q, want first cluster level read-committed", got.Driver.Consistency)
	}
	if len(got.Driver.Cluster.Consistency) != 2 {
		t.Errorf("Cluster.Consistency = %v, want 2 levels", got.Driver.Cluster.Consistency)
	}
}

func TestResolveExplicitConsistencyBeatsClusterList(t *testing.T) {
	o := baseOptions()
	o.Consistency = "serializable"
	got, err := Resolve(o)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Driver.Consistency != "serializable" {
		t.Errorf("Consistency = %q, want serializable", got.Driver.Consistency)
	}
}

func TestResolveRateFromOverride(t *testing.T) {
	o := baseOptions()
	o.Overrides = map[string]any{"ops_per_sec": 5000}
	got, err := Resolve(o)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: 5000}
	if got.Manifest.Rate != want {
		t.Errorf("Rate = %+v, want %+v", got.Manifest.Rate, want)
	}
}
