package cassandradriver

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// hostsEnv enables the integration tests. When unset the tests skip, so the
// default `go test ./targets/cassandra/...` needs no running cluster. It holds a
// comma-separated host:port list, e.g. "127.0.0.1:9042,127.0.0.1:9043".
const hostsEnv = "PROOFLOAD_CASSANDRA_HOSTS"

// integrationConfig returns a driver.Config pointed at the hosts from the
// environment, or skips the test when they are absent.
func integrationConfig(t *testing.T) driver.Config {
	t.Helper()
	raw := os.Getenv(hostsEnv)
	if raw == "" {
		t.Skipf("%s not set; skipping Cassandra integration test", hostsEnv)
	}
	return driver.Config{
		Endpoints:   splitHosts(raw),
		Consistency: "one",
		Cluster:     domain.ClusterSpec{ReplicationFactor: 1},
	}
}

func splitHosts(raw string) []string {
	var out []string
	for _, h := range strings.Split(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			out = append(out, h)
		}
	}
	return out
}

func TestIntegrationReadInsertUpdate(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := New()
	w := domain.Workload{Name: "itest", Mode: domain.ModePerformance}
	if err := d.Schema(ctx, cfg, w); err != nil {
		t.Fatalf("Schema: %v", err)
	}

	conn, err := d.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, w); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	key := time.Now().UnixNano() // unique key per run to avoid cross-run coupling
	v1 := []byte("first-value")
	v2 := []byte("second-value")

	if r := conn.Execute(ctx, domain.Operation{Type: "insert", Key: key, Value: v1, Seq: 1}); r.Err != nil {
		t.Fatalf("insert: %v", r.Err)
	}
	assertRead(t, ctx, conn, key, v1)

	if r := conn.Execute(ctx, domain.Operation{Type: "update", Key: key, Value: v2, Seq: 2}); r.Err != nil {
		t.Fatalf("update: %v", r.Err)
	}
	assertRead(t, ctx, conn, key, v2)

	rs := conn.Execute(ctx, domain.Operation{Type: "scan", Key: key})
	if rs.Err != nil {
		t.Fatalf("scan: %v", rs.Err)
	}
	if _, ok := rs.Observed.([][]byte); !ok {
		t.Fatalf("scan observed = %#v, want [][]byte", rs.Observed)
	}
}

func assertRead(t *testing.T, ctx context.Context, conn driver.Conn, key int64, want []byte) {
	t.Helper()
	r := conn.Execute(ctx, domain.Operation{Type: "read", Key: key})
	if r.Err != nil {
		t.Fatalf("read: %v", r.Err)
	}
	got, ok := r.Observed.([]byte)
	if !ok {
		t.Fatalf("read observed type = %T, want []byte", r.Observed)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read value = %q, want %q", got, want)
	}
}

func TestIntegrationReadKeyFrom(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := New()
	w := domain.Workload{Name: "itest"}
	if err := d.Schema(ctx, cfg, w); err != nil {
		t.Fatalf("Schema: %v", err)
	}
	conn, err := d.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, w); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	key := time.Now().UnixNano()
	val := []byte("replica-check")
	if r := conn.Execute(ctx, domain.Operation{Type: "insert", Key: key, Value: val, Seq: 1}); r.Err != nil {
		t.Fatalf("insert: %v", r.Err)
	}

	ca := d.(driver.ClusterAware)
	node := domain.Node{ID: "n1", Role: domain.RolePrimary, Client: cfg.Endpoints[0]}
	res, err := ca.ReadKeyFrom(ctx, node, key)
	if err != nil {
		t.Fatalf("ReadKeyFrom: %v", err)
	}
	got, ok := res.Observed.([]byte)
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("ReadKeyFrom observed = %#v, want %q", res.Observed, val)
	}
}
