package pgdriver

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
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
		{"read", domain.Operation{Type: "read", Key: 7}, 0, stmtRead, []any{int64(7)}, kindRead, false},
		{"r alias", domain.Operation{Type: "r", Key: 9}, 0, stmtRead, []any{int64(9)}, kindRead, false},
		{"insert upsert", domain.Operation{Type: "insert", Key: 1, Value: val, Seq: 3}, 0, stmtUpsert, []any{int64(1), val, int64(3)}, kindWrite, false},
		{"w alias", domain.Operation{Type: "w", Key: 2, Value: val, Seq: 4}, 0, stmtUpsert, []any{int64(2), val, int64(4)}, kindWrite, false},
		{"update", domain.Operation{Type: "update", Key: 5, Value: val, Seq: 6}, 0, stmtUpdate, []any{int64(5), val, int64(6)}, kindWrite, false},
		{"scan default limit", domain.Operation{Type: "scan", Key: 10}, 0, stmtScan, []any{int64(10), defaultScanLimit}, kindScan, false},
		{"scan explicit limit", domain.Operation{Type: "scan", Key: 10}, 25, stmtScan, []any{int64(10), 25}, kindScan, false},
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

func TestIsoLevel(t *testing.T) {
	tests := []struct {
		consistency string
		want        pgx.TxIsoLevel
		wantErr     bool
	}{
		{"", pgx.ReadCommitted, false},
		{"read-committed", pgx.ReadCommitted, false},
		{"read committed", pgx.ReadCommitted, false},
		{"repeatable-read", pgx.RepeatableRead, false},
		{"serializable", pgx.Serializable, false},
		{"snapshot", "", true},
		{"QUORUM", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.consistency, func(t *testing.T) {
			got, err := isoLevel(tt.consistency)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("isoLevel(%q): expected error", tt.consistency)
				}
				return
			}
			if err != nil {
				t.Fatalf("isoLevel(%q): unexpected error: %v", tt.consistency, err)
			}
			if got != tt.want {
				t.Errorf("isoLevel(%q) = %q, want %q", tt.consistency, got, tt.want)
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
	for _, name := range []string{stmtRead, stmtUpsert, stmtUpdate, stmtScan} {
		if sql, ok := stmts[name]; !ok || sql == "" {
			t.Errorf("statements() missing SQL for %q", name)
		}
	}
}
