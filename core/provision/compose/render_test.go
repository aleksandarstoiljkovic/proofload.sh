package compose

import (
	"strings"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/provision"
)

// TestBackend asserts the provisioner reports and self-registers the compose
// backend.
func TestBackend(t *testing.T) {
	if got := New().Backend(); got != domain.BackendCompose {
		t.Fatalf("Backend() = %q, want %q", got, domain.BackendCompose)
	}
	p, err := provision.Get(domain.BackendCompose)
	if err != nil {
		t.Fatalf("compose backend not registered: %v", err)
	}
	if p.Backend() != domain.BackendCompose {
		t.Fatalf("registered backend = %q", p.Backend())
	}
}

// TestRenderPostgres verifies a 3-node postgres topology renders a valid
// project: three distinct services, distinct container names and host ports,
// and the postgres env + healthcheck from the dedicated hook.
func TestRenderPostgres(t *testing.T) {
	top := domain.Topology{
		Name:    "pg",
		Target:  "postgresql",
		Image:   "postgres",
		Version: "16",
		Nodes:   3,
	}
	m := buildModel(top)
	if len(m.Services) != 3 {
		t.Fatalf("services = %d, want 3", len(m.Services))
	}

	names := map[string]bool{}
	ports := map[int]bool{}
	for i, s := range m.Services {
		if names[s.ContainerName] {
			t.Errorf("duplicate container name %q", s.ContainerName)
		}
		names[s.ContainerName] = true
		if ports[s.HostPort] {
			t.Errorf("duplicate host port %d", s.HostPort)
		}
		ports[s.HostPort] = true
		if s.Image != "postgres:16" {
			t.Errorf("service %d image = %q, want postgres:16", i, s.Image)
		}
	}
	if !names["pg-0"] || !names["pg-1"] || !names["pg-2"] {
		t.Errorf("container names = %v, want pg-0/1/2", names)
	}
	if m.Services[0].Role != domain.RolePrimary || m.Services[1].Role != domain.RoleReplica {
		t.Errorf("roles = %v, %v; want primary, replica",
			m.Services[0].Role, m.Services[1].Role)
	}

	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"name: proofload-pg",
		"container_name: pg-0",
		"container_name: pg-2",
		"POSTGRES_PASSWORD: \"proofload\"",
		"pg_isready -U proofload",
		"\"15432:5432\"",
		"\"15434:5432\"",
		"pg-0-data:/var/lib/postgresql/data",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered YAML missing %q\n---\n%s", want, out)
		}
	}
	assertValidCompose(t, out, 3)
}

// TestRenderGeneric verifies an unknown target still yields a working N-node
// project on the generic path (no env/health, generic port base).
func TestRenderGeneric(t *testing.T) {
	top := domain.Topology{
		Name:    "custom",
		Target:  "mystore",
		Image:   "acme/mystore",
		Version: "1.2",
		Nodes:   2,
		Resources: domain.ResourceSpec{
			CPU:    "0.5",
			Memory: "512M",
		},
	}
	m := buildModel(top)
	if len(m.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(m.Services))
	}
	for i, s := range m.Services {
		if s.Image != "acme/mystore:1.2" {
			t.Errorf("service %d image = %q", i, s.Image)
		}
		if s.Role != domain.RoleGeneric {
			t.Errorf("service %d role = %q, want generic", i, s.Role)
		}
		if s.Health != nil {
			t.Errorf("service %d unexpected healthcheck", i)
		}
		if len(s.Env) != 0 {
			t.Errorf("service %d unexpected env %v", i, s.Env)
		}
	}
	if m.Services[0].HostPort == m.Services[1].HostPort {
		t.Errorf("host ports not distinct: %d", m.Services[0].HostPort)
	}

	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"custom-0:", "custom-1:",
		"image: acme/mystore:1.2",
		"cpus: \"0.5\"",
		"memory: 512M",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered YAML missing %q\n---\n%s", want, out)
		}
	}
	assertValidCompose(t, out, 2)
}

// TestRenderDefaults covers zero-value edge cases: no nodes and no image fall
// back to a single node and the hook's default image.
func TestRenderDefaults(t *testing.T) {
	m := buildModel(domain.Topology{Name: "r", Target: "redis"})
	if len(m.Services) != 1 {
		t.Fatalf("services = %d, want 1 (defaulted)", len(m.Services))
	}
	if got := m.Services[0].Image; got != "redis" {
		t.Errorf("image = %q, want redis (hook default)", got)
	}
	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out, "redis-cli") {
		t.Errorf("redis healthcheck missing:\n%s", out)
	}
}

// assertValidCompose does a lightweight structural check without a YAML
// dependency: a top-level services key, the right service count under it, and
// no tab characters (which YAML forbids).
func assertValidCompose(t *testing.T, out string, wantServices int) {
	t.Helper()
	if strings.Contains(out, "\t") {
		t.Errorf("rendered YAML contains a tab character")
	}
	if !strings.HasPrefix(out, "name: ") {
		t.Errorf("YAML must start with a project name")
	}
	if !strings.Contains(out, "\nservices:\n") {
		t.Errorf("YAML missing services block")
	}
	// Restrict the service-key count to the services block, since the
	// top-level volumes block also carries two-space-indented keys.
	body := out[strings.Index(out, "\nservices:\n")+len("\nservices:\n"):]
	if i := strings.Index(body, "\nvolumes:\n"); i >= 0 {
		body = body[:i]
	}
	got := 0
	for _, line := range strings.Split(body, "\n") {
		// Service keys are indented exactly two spaces under services:.
		if len(line) > 3 && line[0] == ' ' && line[1] == ' ' &&
			line[2] != ' ' && strings.HasSuffix(line, ":") {
			got++
		}
	}
	if got != wantServices {
		t.Errorf("counted %d service keys, want %d\n---\n%s", got, wantServices, out)
	}
}
