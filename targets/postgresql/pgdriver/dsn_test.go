package pgdriver

import (
	"testing"

	"github.com/proofload/proofload/core/driver"
)

func TestBuildDSN(t *testing.T) {
	opts := dsnOptions{User: "app", Password: "secret", DBName: "proofload", SSLMode: "disable"}
	tests := []struct {
		name     string
		endpoint string
		opts     dsnOptions
		want     string
	}{
		{
			name:     "host and port",
			endpoint: "db.internal:6543",
			opts:     opts,
			want:     "host=db.internal port=6543 user=app dbname=proofload sslmode=disable password=secret",
		},
		{
			name:     "host without port defaults 5432",
			endpoint: "localhost",
			opts:     opts,
			want:     "host=localhost port=5432 user=app dbname=proofload sslmode=disable password=secret",
		},
		{
			name:     "no password omits keyword",
			endpoint: "localhost:5432",
			opts:     dsnOptions{User: "app", DBName: "proofload", SSLMode: "disable"},
			want:     "host=localhost port=5432 user=app dbname=proofload sslmode=disable",
		},
		{
			name:     "url form passed through",
			endpoint: "postgres://u:p@host:5432/db?sslmode=require",
			opts:     opts,
			want:     "postgres://u:p@host:5432/db?sslmode=require",
		},
		{
			name:     "keyword dsn passed through",
			endpoint: "host=other port=5432 dbname=x",
			opts:     opts,
			want:     "host=other port=5432 dbname=x",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildDSN(tt.endpoint, tt.opts); got != tt.want {
				t.Errorf("buildDSN(%q)\n  got  %q\n  want %q", tt.endpoint, got, tt.want)
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
		{"host", "host", defaultPGPort},
		{"", "localhost", defaultPGPort},
		{"127.0.0.1:5555", "127.0.0.1", "5555"},
		{"[::1]", "[::1]", defaultPGPort},
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

func TestResolveOptionsPasswordFromEnv(t *testing.T) {
	t.Setenv("PROOFLOAD_PG_PASSWORD", "envpw")
	t.Setenv("PGPASSWORD", "ignored")
	got := resolveOptions(map[string]any{"user": "u1", "dbname": "d1"})
	if got.Password != "envpw" {
		t.Errorf("Password = %q, want envpw (PROOFLOAD_PG_PASSWORD wins)", got.Password)
	}
	if got.User != "u1" {
		t.Errorf("User = %q, want u1 (param wins)", got.User)
	}
	if got.DBName != "d1" {
		t.Errorf("DBName = %q, want d1", got.DBName)
	}
	if got.SSLMode != defaultSSLMode {
		t.Errorf("SSLMode = %q, want default %q", got.SSLMode, defaultSSLMode)
	}
}

func TestResolveOptionsDefaults(t *testing.T) {
	t.Setenv("PROOFLOAD_PG_PASSWORD", "")
	t.Setenv("PGPASSWORD", "")
	t.Setenv("PGUSER", "")
	t.Setenv("PGDATABASE", "")
	t.Setenv("PGSSLMODE", "")
	got := resolveOptions(nil)
	if got.User != defaultUser || got.DBName != defaultDBName || got.SSLMode != defaultSSLMode {
		t.Errorf("defaults not applied: %+v", got)
	}
	if got.Password != "" {
		t.Errorf("Password = %q, want empty", got.Password)
	}
}

func TestNewDriverIdentity(t *testing.T) {
	d := New()
	if d.Name() != "postgresql" {
		t.Errorf("Name() = %q, want postgresql", d.Name())
	}
	ca, ok := d.(driver.ClusterAware)
	if !ok {
		t.Fatal("driver does not implement driver.ClusterAware")
	}
	if got := ca.ConsistencyLevels(); len(got) != 3 || got[0] != "read-committed" || got[2] != "serializable" {
		t.Errorf("ConsistencyLevels() = %v", got)
	}
}
