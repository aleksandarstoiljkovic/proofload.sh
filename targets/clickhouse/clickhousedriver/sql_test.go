package clickhousedriver

import (
	"reflect"
	"strings"
	"testing"

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
		{"read", domain.Operation{Type: "read", Key: 7}, 0, readSQL, []any{int64(7)}, kindRead, false},
		{"r alias", domain.Operation{Type: "r", Key: 9}, 0, readSQL, []any{int64(9)}, kindRead, false},
		{"insert", domain.Operation{Type: "insert", Key: 1, Value: val, Seq: 3}, 0, insertSQL, []any{int64(1), "payload", int64(3)}, kindWrite, false},
		{"w alias", domain.Operation{Type: "w", Key: 2, Value: val, Seq: 4}, 0, insertSQL, []any{int64(2), "payload", int64(4)}, kindWrite, false},
		{"update is insert", domain.Operation{Type: "update", Key: 5, Value: val, Seq: 6}, 0, insertSQL, []any{int64(5), "payload", int64(6)}, kindWrite, false},
		{"scan default limit", domain.Operation{Type: "scan", Key: 10}, 0, scanSQL, []any{int64(10), defaultScanLimit}, kindScan, false},
		{"scan explicit limit", domain.Operation{Type: "scan", Key: 10}, 25, scanSQL, []any{int64(10), 25}, kindScan, false},
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

func TestCreateTableSQL(t *testing.T) {
	tests := []struct {
		name      string
		clustered bool
		wantSubs  []string
		notWant   []string
	}{
		{
			name:      "standalone",
			clustered: false,
			wantSubs: []string{
				"CREATE TABLE IF NOT EXISTS proofload_kv",
				"(k Int64, v String, seq Int64)",
				"ENGINE = ReplacingMergeTree(seq)",
				"ORDER BY k",
			},
			notWant: []string{"ON CLUSTER", "Replicated"},
		},
		{
			name:      "clustered",
			clustered: true,
			wantSubs: []string{
				"CREATE TABLE IF NOT EXISTS proofload_kv ON CLUSTER 'proofload'",
				"ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/proofload_kv', '{replica}', seq)",
				"ORDER BY k",
			},
			notWant: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createTableSQL(tt.clustered)
			for _, sub := range tt.wantSubs {
				if !strings.Contains(got, sub) {
					t.Errorf("createTableSQL(%v) = %q\n  missing %q", tt.clustered, got, sub)
				}
			}
			for _, sub := range tt.notWant {
				if strings.Contains(got, sub) {
					t.Errorf("createTableSQL(%v) = %q\n  should not contain %q", tt.clustered, got, sub)
				}
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
	for _, name := range []string{stmtRead, stmtInsert, stmtScan} {
		if sql, ok := stmts[name]; !ok || sql == "" {
			t.Errorf("statements() missing SQL for %q", name)
		}
	}
}
