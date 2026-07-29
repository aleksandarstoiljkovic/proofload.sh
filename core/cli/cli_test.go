package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/proofload/proofload/core/testutil"
)

// testAssets is a minimal in-memory asset tree matching the fixed target.yaml /
// workload.yaml schemas, used to exercise the assembly layer without a database.
func testAssets() fstest.MapFS {
	return fstest.MapFS{
		"target.yaml": &fstest.MapFile{Data: []byte(`
target: fake
image: fake
version: "1"
defaults: {connections: 4, warmup: 1s, duration: 1s}
cluster: {backend: external, nodes: 1, replication_factor: 1, consistency: [read-committed]}
capabilities: {verify_models: [reconciliation], faults: []}
`)},
		"workloads/smoke.yaml": &fstest.MapFile{Data: []byte(`
name: smoke
mode: performance
key_space: 100
key_dist: uniform
value_size: 8
verify_model: ""
operations: [{type: read, weight: 100}]
params: {}
`)},
		"schema/schema.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}
}

// TestMaterialize copies an embedded asset tree to disk preserving structure.
func TestMaterialize(t *testing.T) {
	dir, cleanup, err := materialize(testAssets())
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	defer cleanup()

	for _, rel := range []string{"target.yaml", "workloads/smoke.yaml", "schema/schema.sql"} {
		if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
			t.Errorf("expected %s on disk: %v", rel, err)
		}
	}
	cleanup()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("cleanup did not remove %s", dir)
	}
}

// TestListWorkloads prints the embedded workloads.
func TestListWorkloads(t *testing.T) {
	e := Engine{Name: "fake", Driver: testutil.NewFakeDriver(), Assets: testAssets()}
	cmd := listWorkloadsCmd(e)
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("list-workloads: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "smoke" {
		t.Errorf("list-workloads = %q, want smoke", got)
	}
}

// TestRunRequiresEndpoints verifies the run wiring resolves config and fails
// cleanly before touching the driver when no endpoints are supplied.
func TestRunRequiresEndpoints(t *testing.T) {
	e := Engine{Name: "fake", Driver: testutil.NewFakeDriver(), Assets: testAssets()}
	cmd := runCmd(e)
	cmd.SetArgs([]string{"--workload", "smoke"})
	cmd.SetOut(&bytes.Buffer{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "no endpoints") {
		t.Fatalf("want 'no endpoints' error, got %v", err)
	}
}

// TestRunUnknownWorkload surfaces a clear error for a missing workload file.
func TestRunUnknownWorkload(t *testing.T) {
	e := Engine{Name: "fake", Driver: testutil.NewFakeDriver(), Assets: testAssets()}
	cmd := runCmd(e)
	cmd.SetArgs([]string{"--workload", "does-not-exist", "--endpoints", "127.0.0.1:5432"})
	cmd.SetOut(&bytes.Buffer{})
	if err := cmd.Execute(); err == nil {
		t.Fatal("want error for unknown workload, got nil")
	}
}
