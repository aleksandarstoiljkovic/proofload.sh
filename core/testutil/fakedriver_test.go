package testutil_test

import (
	"context"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/testutil"
)

// TestFakeDriverContract exercises the frozen driver.Driver/Conn contract end to
// end, proving the foundation compiles and behaves as documented.
func TestFakeDriverContract(t *testing.T) {
	var d driver.Driver = testutil.NewFakeDriver()
	if d.Name() != "fake" {
		t.Fatalf("Name = %q, want fake", d.Name())
	}

	ctx := context.Background()
	w := domain.Workload{Name: "smoke", Mode: domain.ModePerformance}
	if err := d.Schema(ctx, driver.Config{}, w); err != nil {
		t.Fatalf("Schema: %v", err)
	}

	conn, err := d.Connect(ctx, driver.Config{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, w); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// Write then read back the same key.
	write := domain.Operation{Type: "insert", Key: 7, Value: []byte("v")}
	if res := conn.Execute(ctx, write); res.Err != nil {
		t.Fatalf("write Execute: %v", res.Err)
	}
	read := domain.Operation{Type: "read", Key: 7}
	res := conn.Execute(ctx, read)
	if res.Err != nil {
		t.Fatalf("read Execute: %v", res.Err)
	}
	got, _ := res.Observed.([]byte)
	if string(got) != "v" {
		t.Fatalf("read Observed = %q, want v", got)
	}

	if fd := d.(*testutil.FakeDriver); len(fd.Operations()) != 2 {
		t.Fatalf("recorded %d ops, want 2", len(fd.Operations()))
	}
}

// TestRegistry verifies driver registration and lookup.
func TestRegistry(t *testing.T) {
	driver.Register(&registryFake{})
	if _, err := driver.Get("registry-fake"); err != nil {
		t.Fatalf("Get after Register: %v", err)
	}
	if _, err := driver.Get("nope"); err == nil {
		t.Fatal("Get unknown target: want error, got nil")
	}
}

type registryFake struct{ *testutil.FakeDriver }

func (registryFake) Name() string { return "registry-fake" }
