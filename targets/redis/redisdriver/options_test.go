package redisdriver

import (
	"testing"

	"github.com/proofload/proofload/core/driver"
)

func TestNormalizeAddr(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"redis.internal:6380", "redis.internal:6380"},
		{"redis.internal", "redis.internal:6379"},
		{"", "localhost:6379"},
		{"127.0.0.1:7000", "127.0.0.1:7000"},
		{"  spaced:6379  ", "spaced:6379"},
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
		{"host", "host", defaultRedisPort},
		{"", "localhost", defaultRedisPort},
		{"127.0.0.1:5555", "127.0.0.1", "5555"},
		{"[::1]", "[::1]", defaultRedisPort},
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
	t.Setenv("PROOFLOAD_REDIS_PASSWORD", "envpw")
	t.Setenv("REDIS_PASSWORD", "ignored")
	got := resolveOptions("cache:6379", nil)
	if got.Password != "envpw" {
		t.Errorf("Password = %q, want envpw (PROOFLOAD_REDIS_PASSWORD wins)", got.Password)
	}
	if got.Addr != "cache:6379" {
		t.Errorf("Addr = %q, want cache:6379", got.Addr)
	}
	if got.DB != 0 {
		t.Errorf("DB = %d, want 0", got.DB)
	}
}

func TestResolveOptionsPasswordFallback(t *testing.T) {
	t.Setenv("PROOFLOAD_REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD", "fallbackpw")
	got := resolveOptions("localhost", nil)
	if got.Password != "fallbackpw" {
		t.Errorf("Password = %q, want fallbackpw (REDIS_PASSWORD fallback)", got.Password)
	}
	if got.Addr != "localhost:6379" {
		t.Errorf("Addr = %q, want localhost:6379", got.Addr)
	}
}

func TestNewDriverIdentity(t *testing.T) {
	d := New()
	if d.Name() != "redis" {
		t.Errorf("Name() = %q, want redis", d.Name())
	}
	ca, ok := d.(driver.ClusterAware)
	if !ok {
		t.Fatal("driver does not implement driver.ClusterAware")
	}
	got := ca.ConsistencyLevels()
	if len(got) != 2 || got[0] != consNone || got[1] != consWait {
		t.Errorf("ConsistencyLevels() = %v, want [none wait]", got)
	}
}
