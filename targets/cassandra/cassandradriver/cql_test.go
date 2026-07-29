package cassandradriver

import (
	"reflect"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/domain"
)

func TestPlanFor(t *testing.T) {
	val := []byte("payload")
	tests := []struct {
		name     string
		op       domain.Operation
		scan     int
		wantStmt string
		wantArgs []any
		wantKind opKind
		wantErr  bool
	}{
		{"read", domain.Operation{Type: "read", Key: 7}, 0, readCQL, []any{int64(7)}, kindRead, false},
		{"r alias", domain.Operation{Type: "r", Key: 9}, 0, readCQL, []any{int64(9)}, kindRead, false},
		{"insert", domain.Operation{Type: "insert", Key: 1, Value: val, Seq: 3}, 0, insertCQL, []any{int64(1), val, int64(3)}, kindWrite, false},
		{"w alias", domain.Operation{Type: "w", Key: 2, Value: val, Seq: 4}, 0, insertCQL, []any{int64(2), val, int64(4)}, kindWrite, false},
		{"update", domain.Operation{Type: "update", Key: 5, Value: val, Seq: 6}, 0, updateCQL, []any{val, int64(6), int64(5)}, kindWrite, false},
		{"scan default limit", domain.Operation{Type: "scan", Key: 10}, 0, scanCQL, []any{int64(10), defaultScanLimit}, kindScan, false},
		{"scan explicit limit", domain.Operation{Type: "scan", Key: 10}, 25, scanCQL, []any{int64(10), 25}, kindScan, false},
		{"unknown", domain.Operation{Type: "bogus"}, 0, "", nil, kindRead, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := planFor(tt.op, tt.scan)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("planFor(%q): expected error, got nil", tt.op.Type)
				}
				return
			}
			if err != nil {
				t.Fatalf("planFor(%q): unexpected error: %v", tt.op.Type, err)
			}
			if got.stmt != tt.wantStmt {
				t.Errorf("stmt = %q, want %q", got.stmt, tt.wantStmt)
			}
			if got.kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, tt.wantKind)
			}
			if !reflect.DeepEqual(got.args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", got.args, tt.wantArgs)
			}
		})
	}
}

func TestConsistencyLevel(t *testing.T) {
	tests := []struct {
		consistency string
		want        gocql.Consistency
		wantErr     bool
	}{
		{"", gocql.Quorum, false},
		{"quorum", gocql.Quorum, false},
		{"one", gocql.One, false},
		{"local_quorum", gocql.LocalQuorum, false},
		{"local-quorum", gocql.LocalQuorum, false},
		{"all", gocql.All, false},
		{"serializable", 0, true},
		{"two", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.consistency, func(t *testing.T) {
			got, err := consistencyLevel(tt.consistency)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("consistencyLevel(%q): expected error", tt.consistency)
				}
				return
			}
			if err != nil {
				t.Fatalf("consistencyLevel(%q): unexpected error: %v", tt.consistency, err)
			}
			if got != tt.want {
				t.Errorf("consistencyLevel(%q) = %v, want %v", tt.consistency, got, tt.want)
			}
		})
	}
}

func TestScanLimitFromWorkload(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]any
		want   int
	}{
		{"nil params", nil, defaultScanLimit},
		{"empty params", map[string]any{}, defaultScanLimit},
		{"scan_limit int", map[string]any{"scan_limit": 50}, 50},
		{"scan_limit float", map[string]any{"scan_limit": float64(30)}, 30},
		{"limit alias", map[string]any{"limit": int64(15)}, 15},
		{"non-positive falls back", map[string]any{"scan_limit": 0}, defaultScanLimit},
		{"wrong type falls back", map[string]any{"scan_limit": "big"}, defaultScanLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := domain.Workload{Params: tt.params}
			if got := scanLimitFromWorkload(w); got != tt.want {
				t.Errorf("scanLimitFromWorkload = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestStatementsCoverAllHotOps(t *testing.T) {
	stmts := statements()
	for _, name := range []string{stmtRead, stmtInsert, stmtUpdate, stmtScan} {
		if cql, ok := stmts[name]; !ok || cql == "" {
			t.Errorf("statements() missing CQL for %q", name)
		}
	}
}

func TestCreateKeyspaceCQL(t *testing.T) {
	tests := []struct {
		name     string
		keyspace string
		rf       int
		wantSub  []string
	}{
		{"rf1", "proofload", 1, []string{"CREATE KEYSPACE IF NOT EXISTS proofload", "SimpleStrategy", "'replication_factor': 1"}},
		{"rf3", "proofload", 3, []string{"'replication_factor': 3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createKeyspaceCQL(tt.keyspace, tt.rf)
			for _, sub := range tt.wantSub {
				if !strings.Contains(got, sub) {
					t.Errorf("createKeyspaceCQL(%q,%d) = %q, missing %q", tt.keyspace, tt.rf, got, sub)
				}
			}
		})
	}
}

func TestCreateTableCQL(t *testing.T) {
	got := createTableCQL("proofload")
	for _, sub := range []string{
		"CREATE TABLE IF NOT EXISTS proofload.kv",
		"k bigint PRIMARY KEY",
		"v blob",
		"seq bigint",
	} {
		if !strings.Contains(got, sub) {
			t.Errorf("createTableCQL = %q, missing %q", got, sub)
		}
	}
}

// TestScanUsesTokenRange guards the token-range scan caveat: the scan must not
// restrict the partition key by value (which would require ALLOW FILTERING).
func TestScanUsesTokenRange(t *testing.T) {
	if !strings.Contains(scanCQL, "token(k) >= token(?)") {
		t.Errorf("scanCQL = %q, want a token-range restriction", scanCQL)
	}
	if strings.Contains(scanCQL, "ALLOW FILTERING") {
		t.Errorf("scanCQL = %q, must not use ALLOW FILTERING", scanCQL)
	}
}
