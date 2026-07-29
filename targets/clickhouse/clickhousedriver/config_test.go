package clickhousedriver

import (
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

func TestConsistencySettings(t *testing.T) {
	tests := []struct {
		consistency string
		wantNil     bool
		wantErr     bool
	}{
		{"", true, false},
		{"none", true, false},
		{"quorum", false, false},
		{"serializable", false, true},
		{"ONE", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.consistency, func(t *testing.T) {
			got, err := consistencySettings(tt.consistency)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("consistencySettings(%q): expected error", tt.consistency)
				}
				return
			}
			if err != nil {
				t.Fatalf("consistencySettings(%q): unexpected error: %v", tt.consistency, err)
			}
			if tt.wantNil {
				if got != nil {
					t.Errorf("consistencySettings(%q) = %v, want nil", tt.consistency, got)
				}
				return
			}
			if got["insert_quorum"] != 2 || got["select_sequential_consistency"] != 1 {
				t.Errorf("consistencySettings(%q) = %v, want quorum settings", tt.consistency, got)
			}
		})
	}
}

func TestIsClustered(t *testing.T) {
	twoNodes := domain.ClusterSpec{Nodes: []domain.Node{{ID: "ch1"}, {ID: "ch2"}}}
	oneNode := domain.ClusterSpec{Nodes: []domain.Node{{ID: "ch1"}}}
	tests := []struct {
		name string
		cfg  driver.Config
		want bool
	}{
		{"no nodes standalone", driver.Config{}, false},
		{"single node standalone", driver.Config{Cluster: oneNode}, false},
		{"multi node clustered", driver.Config{Cluster: twoNodes}, true},
		{"cluster param override", driver.Config{Params: map[string]any{"cluster": "proofload"}}, true},
		{"cluster param present empty", driver.Config{Params: map[string]any{"cluster": ""}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isClustered(tt.cfg); got != tt.want {
				t.Errorf("isClustered = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeAddr(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"ch1:9000", "ch1:9000"},
		{"ch1", "ch1:9000"},
		{"", "localhost:9000"},
		{"127.0.0.1:19000", "127.0.0.1:19000"},
		{"  ch2:9001  ", "ch2:9001"},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			if got := normalizeAddr(tt.endpoint); got != tt.want {
				t.Errorf("normalizeAddr(%q) = %q, want %q", tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestSplitHostPort(t *testing.T) {
	tests := []struct {
		endpoint string
		wantHost string
		wantPort string
	}{
		{"host:1234", "host", "1234"},
		{"host", "host", defaultCHPort},
		{"", "localhost", defaultCHPort},
		{"127.0.0.1:9000", "127.0.0.1", "9000"},
		{"[::1]", "[::1]", defaultCHPort},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			h, p := splitHostPort(tt.endpoint)
			if h != tt.wantHost || p != tt.wantPort {
				t.Errorf("splitHostPort(%q) = (%q,%q), want (%q,%q)", tt.endpoint, h, p, tt.wantHost, tt.wantPort)
			}
		})
	}
}

func TestResolveAuth(t *testing.T) {
	t.Run("defaults with no env", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_USER", "")
		t.Setenv("PROOFLOAD_CLICKHOUSE_PASSWORD", "")
		t.Setenv("CLICKHOUSE_PASSWORD", "")
		got := resolveAuth(nil)
		if got.Database != defaultDatabase || got.Username != defaultUser || got.Password != "" {
			t.Errorf("resolveAuth defaults = %+v", got)
		}
	})
	t.Run("params and env override", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_USER", "loader")
		t.Setenv("PROOFLOAD_CLICKHOUSE_PASSWORD", "envpw")
		t.Setenv("CLICKHOUSE_PASSWORD", "ignored")
		got := resolveAuth(map[string]any{"dbname": "bench"})
		if got.Database != "bench" {
			t.Errorf("Database = %q, want bench", got.Database)
		}
		if got.Username != "loader" {
			t.Errorf("Username = %q, want loader", got.Username)
		}
		if got.Password != "envpw" {
			t.Errorf("Password = %q, want envpw (PROOFLOAD_CLICKHOUSE_PASSWORD wins)", got.Password)
		}
	})
}

func TestNewDriverIdentity(t *testing.T) {
	d := New()
	if d.Name() != "clickhouse" {
		t.Errorf("Name() = %q, want clickhouse", d.Name())
	}
	ca, ok := d.(driver.ClusterAware)
	if !ok {
		t.Fatal("driver does not implement driver.ClusterAware")
	}
	got := ca.ConsistencyLevels()
	if len(got) != 2 || got[0] != "none" || got[1] != "quorum" {
		t.Errorf("ConsistencyLevels() = %v, want [none quorum]", got)
	}
}
