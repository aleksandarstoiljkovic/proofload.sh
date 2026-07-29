package redisdriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

func TestKeyName(t *testing.T) {
	tests := []struct {
		key  int64
		want string
	}{
		{0, "proofload:{0}"},
		{7, "proofload:{7}"},
		{1000000, "proofload:{1000000}"},
	}
	for _, tt := range tests {
		if got := keyName(tt.key); got != tt.want {
			t.Errorf("keyName(%d) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestPlanFor(t *testing.T) {
	val := []byte("payload")
	tests := []struct {
		name     string
		op       domain.Operation
		scan     int
		wantKind cmdKind
		wantKey  string
		wantKeys []string
		wantVal  []byte
		wantErr  bool
	}{
		{"read", domain.Operation{Type: "read", Key: 7}, 0, cmdGet, "proofload:{7}", nil, nil, false},
		{"r alias", domain.Operation{Type: "r", Key: 9}, 0, cmdGet, "proofload:{9}", nil, nil, false},
		{"insert set", domain.Operation{Type: "insert", Key: 1, Value: val}, 0, cmdSet, "proofload:{1}", nil, val, false},
		{"w alias", domain.Operation{Type: "w", Key: 2, Value: val}, 0, cmdSet, "proofload:{2}", nil, val, false},
		{"update set", domain.Operation{Type: "update", Key: 5, Value: val}, 0, cmdSet, "proofload:{5}", nil, val, false},
		{"scan default limit", domain.Operation{Type: "scan", Key: 10}, 0, cmdScan, "", scanKeys(10, defaultScanLimit), nil, false},
		{"scan explicit limit", domain.Operation{Type: "scan", Key: 10}, 3, cmdScan, "", []string{"proofload:{10}", "proofload:{11}", "proofload:{12}"}, nil, false},
		{"unknown", domain.Operation{Type: "bogus"}, 0, cmdGet, "", nil, nil, true},
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
			if got.kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, tt.wantKind)
			}
			if got.key != tt.wantKey {
				t.Errorf("key = %q, want %q", got.key, tt.wantKey)
			}
			if !reflect.DeepEqual(got.keys, tt.wantKeys) {
				t.Errorf("keys = %#v, want %#v", got.keys, tt.wantKeys)
			}
			if !reflect.DeepEqual(got.value, tt.wantVal) {
				t.Errorf("value = %#v, want %#v", got.value, tt.wantVal)
			}
		})
	}
}

func TestParseConsistency(t *testing.T) {
	tests := []struct {
		name         string
		level        string
		params       map[string]any
		wantLevel    string
		wantReplicas int
		wantTimeout  time.Duration
		wantErr      bool
	}{
		{"empty defaults none", "", nil, consNone, 0, 0, false},
		{"none", "none", nil, consNone, 0, 0, false},
		{"wait defaults", "wait", nil, consWait, defaultWaitReplicas, defaultWaitTimeout, false},
		{"waitN alias", "waitN", nil, consWait, defaultWaitReplicas, defaultWaitTimeout, false},
		{"wait with params", "wait", map[string]any{"wait_replicas": 2, "wait_timeout_ms": 250}, consWait, 2, 250 * time.Millisecond, false},
		{"wait numreplicas alias", "wait", map[string]any{"numreplicas": float64(3)}, consWait, 3, defaultWaitTimeout, false},
		{"unknown rejected", "quorum", nil, "", 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConsistency(tt.level, tt.params)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseConsistency(%q): expected error", tt.level)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseConsistency(%q): unexpected error: %v", tt.level, err)
			}
			if got.level != tt.wantLevel {
				t.Errorf("level = %q, want %q", got.level, tt.wantLevel)
			}
			if got.replicas != tt.wantReplicas {
				t.Errorf("replicas = %d, want %d", got.replicas, tt.wantReplicas)
			}
			if got.timeout != tt.wantTimeout {
				t.Errorf("timeout = %v, want %v", got.timeout, tt.wantTimeout)
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
