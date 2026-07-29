package clickhousedriver

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// addrEnv is the environment variable that enables the integration tests. When
// it is unset the tests skip, so the default `go test ./...` needs no server.
// It carries a native-protocol host:port, e.g. "localhost:19000".
const addrEnv = "PROOFLOAD_CLICKHOUSE_ADDR"

// integrationConfig returns a driver.Config pointed at the address from the
// environment, or skips the test when it is absent.
func integrationConfig(t *testing.T) driver.Config {
	t.Helper()
	addr := os.Getenv(addrEnv)
	if addr == "" {
		t.Skipf("%s not set; skipping ClickHouse integration test", addrEnv)
	}
	return driver.Config{Endpoints: []string{addr}, Consistency: "none"}
}

func TestIntegrationReadInsertUpdate(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// update is an append with a higher seq; read must observe the latest write.
	if r := conn.Execute(ctx, domain.Operation{Type: "update", Key: key, Value: v2, Seq: 2}); r.Err != nil {
		t.Fatalf("update: %v", r.Err)
	}
	assertRead(t, ctx, conn, key, v2)

	rs := conn.Execute(ctx, domain.Operation{Type: "scan", Key: key})
	if rs.Err != nil {
		t.Fatalf("scan: %v", rs.Err)
	}
	if rows, ok := rs.Observed.([][]byte); !ok || len(rows) == 0 {
		t.Fatalf("scan observed = %#v, want non-empty [][]byte", rs.Observed)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	node := domain.Node{ID: "ch1", Role: domain.RoleReplica, Client: os.Getenv(addrEnv)}
	res, err := ca.ReadKeyFrom(ctx, node, key)
	if err != nil {
		t.Fatalf("ReadKeyFrom: %v", err)
	}
	got, ok := res.Observed.([]byte)
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("ReadKeyFrom observed = %#v, want %q", res.Observed, val)
	}
}
