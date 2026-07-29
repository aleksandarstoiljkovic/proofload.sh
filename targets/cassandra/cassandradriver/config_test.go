package cassandradriver

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

func TestHostsFrom(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []string
		want      []string
	}{
		{"empty defaults to localhost", nil, []string{defaultHost}},
		{"passthrough", []string{"a:9042", "b:9042"}, []string{"a:9042", "b:9042"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hostsFrom(tt.endpoints)
			if len(got) != len(tt.want) {
				t.Fatalf("hostsFrom(%v) = %v, want %v", tt.endpoints, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("hostsFrom(%v)[%d] = %q, want %q", tt.endpoints, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestResolveReplicationFactor(t *testing.T) {
	tests := []struct {
		name string
		rf   int
		want int
	}{
		{"unset defaults to 1", 0, 1},
		{"negative defaults to 1", -2, 1},
		{"configured", 3, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := driver.Config{Cluster: domain.ClusterSpec{ReplicationFactor: tt.rf}}
			if got := resolveReplicationFactor(cfg); got != tt.want {
				t.Errorf("resolveReplicationFactor(%d) = %d, want %d", tt.rf, got, tt.want)
			}
		})
	}
}

func TestResolveKeyspace(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]any
		want   string
	}{
		{"default", nil, keyspaceName},
		{"override", map[string]any{"keyspace": "ks2"}, "ks2"},
		{"blank falls back", map[string]any{"keyspace": ""}, keyspaceName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := driver.Config{Params: tt.params}
			if got := resolveKeyspace(cfg); got != tt.want {
				t.Errorf("resolveKeyspace = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveOptions(t *testing.T) {
	t.Setenv("PROOFLOAD_CASSANDRA_USER", "")
	t.Setenv("PROOFLOAD_CASSANDRA_PASSWORD", "")

	t.Run("defaults to quorum", func(t *testing.T) {
		got, err := resolveOptions(driver.Config{}, keyspaceName)
		if err != nil {
			t.Fatalf("resolveOptions: %v", err)
		}
		if got.Consistency != gocql.Quorum {
			t.Errorf("Consistency = %v, want QUORUM", got.Consistency)
		}
		if got.Keyspace != keyspaceName {
			t.Errorf("Keyspace = %q, want %q", got.Keyspace, keyspaceName)
		}
	})

	t.Run("bad consistency errors", func(t *testing.T) {
		if _, err := resolveOptions(driver.Config{Consistency: "nope"}, keyspaceName); err == nil {
			t.Fatal("expected error for unsupported consistency")
		}
	})

	t.Run("password from env", func(t *testing.T) {
		t.Setenv("PROOFLOAD_CASSANDRA_USER", "cass")
		t.Setenv("PROOFLOAD_CASSANDRA_PASSWORD", "secret")
		got, err := resolveOptions(driver.Config{}, keyspaceName)
		if err != nil {
			t.Fatalf("resolveOptions: %v", err)
		}
		if got.User != "cass" || got.Password != "secret" {
			t.Errorf("auth = %q/%q, want cass/secret", got.User, got.Password)
		}
	})
}

func TestBuildClusterAppliesOptions(t *testing.T) {
	opts := clusterOptions{
		Keyspace:    "proofload",
		Consistency: gocql.LocalQuorum,
		User:        "u",
		Password:    "p",
	}
	cluster := buildCluster([]string{"h1:9042", "h2:9042"}, opts)
	if cluster.Keyspace != "proofload" {
		t.Errorf("Keyspace = %q, want proofload", cluster.Keyspace)
	}
	if cluster.Consistency != gocql.LocalQuorum {
		t.Errorf("Consistency = %v, want LOCAL_QUORUM", cluster.Consistency)
	}
	if cluster.NumConns != 1 {
		t.Errorf("NumConns = %d, want 1 (runner drives concurrency)", cluster.NumConns)
	}
	if len(cluster.Hosts) != 2 {
		t.Errorf("Hosts = %v, want 2 entries", cluster.Hosts)
	}
	if _, ok := cluster.Authenticator.(gocql.PasswordAuthenticator); !ok {
		t.Errorf("Authenticator = %T, want PasswordAuthenticator", cluster.Authenticator)
	}
}

func TestNewDriverIdentity(t *testing.T) {
	d := New()
	if d.Name() != "cassandra" {
		t.Errorf("Name() = %q, want cassandra", d.Name())
	}
	ca, ok := d.(driver.ClusterAware)
	if !ok {
		t.Fatal("driver does not implement driver.ClusterAware")
	}
	got := ca.ConsistencyLevels()
	if len(got) != 4 || got[0] != "one" || got[3] != "all" {
		t.Errorf("ConsistencyLevels() = %v", got)
	}
}
