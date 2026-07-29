package k8s

import (
	"strings"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/provision"
)

// TestBackend asserts the provisioner reports and self-registers the
// kubernetes backend.
func TestBackend(t *testing.T) {
	if got := New().Backend(); got != domain.BackendKubernetes {
		t.Fatalf("Backend() = %q, want %q", got, domain.BackendKubernetes)
	}
	p, err := provision.Get(domain.BackendKubernetes)
	if err != nil {
		t.Fatalf("kubernetes backend not registered: %v", err)
	}
	if p.Backend() != domain.BackendKubernetes {
		t.Fatalf("registered backend = %q", p.Backend())
	}
}

// TestRenderPostgres verifies a 3-node postgres topology renders valid
// manifests: the namespace, a headless service, a StatefulSet with 3 replicas,
// and the postgres env + pg_isready readiness probe from the dedicated hook.
func TestRenderPostgres(t *testing.T) {
	top := domain.Topology{
		Name:    "pg",
		Target:  "postgresql",
		Image:   "postgres",
		Version: "16",
		Nodes:   3,
	}
	m := buildModel(top)
	if m.Replicas != 3 {
		t.Fatalf("replicas = %d, want 3", m.Replicas)
	}
	if m.Namespace != "proofload-pg" {
		t.Fatalf("namespace = %q, want proofload-pg", m.Namespace)
	}
	if m.Image != "postgres:16" {
		t.Fatalf("image = %q, want postgres:16", m.Image)
	}
	if m.Roles[0] != domain.RolePrimary || m.Roles[1] != domain.RoleReplica {
		t.Errorf("roles = %v, %v; want primary, replica", m.Roles[0], m.Roles[1])
	}

	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"kind: Namespace",
		"name: proofload-pg",
		"kind: Service",
		"clusterIP: None",
		"kind: StatefulSet",
		"serviceName: pg",
		"replicas: 3",
		"image: postgres:16",
		"containerPort: 5432",
		"- name: POSTGRES_PASSWORD",
		`value: "proofload"`,
		"readinessProbe:",
		`command: ["pg_isready", "-U", "proofload"]`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered YAML missing %q\n---\n%s", want, out)
		}
	}
	assertValidManifest(t, out, 3)

	// The resolved nodes must carry in-cluster DNS clients and k8s control
	// endpoints pointing at each pod.
	nodes := m.nodes()
	if len(nodes) != 3 {
		t.Fatalf("nodes = %d, want 3", len(nodes))
	}
	first := nodes[0]
	if first.ID != "pg-0" {
		t.Errorf("node 0 id = %q, want pg-0", first.ID)
	}
	wantClient := "pg-0.pg.proofload-pg.svc.cluster.local:5432"
	if first.Client != wantClient {
		t.Errorf("node 0 client = %q, want %q", first.Client, wantClient)
	}
	if first.Control.Method != domain.ControlK8s ||
		first.Control.Ref != "pg-0" || first.Control.Namespace != "proofload-pg" {
		t.Errorf("node 0 control = %+v", first.Control)
	}
}

// TestRenderGeneric verifies an unknown target still yields a working N-replica
// StatefulSet on the generic path with per-node resource limits.
func TestRenderGeneric(t *testing.T) {
	top := domain.Topology{
		Name:    "custom",
		Target:  "mystore",
		Image:   "acme/mystore",
		Version: "1.2",
		Nodes:   2,
		Resources: domain.ResourceSpec{
			CPU:    "0.5",
			Memory: "512Mi",
		},
	}
	m := buildModel(top)
	if m.Replicas != 2 {
		t.Fatalf("replicas = %d, want 2", m.Replicas)
	}
	for i, r := range m.Roles {
		if r != domain.RoleGeneric {
			t.Errorf("role %d = %q, want generic", i, r)
		}
	}
	if len(m.Env) != 0 {
		t.Errorf("unexpected env %v", m.Env)
	}

	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"kind: StatefulSet",
		"replicas: 2",
		"image: acme/mystore:1.2",
		"containerPort: 8080",
		`cpu: "0.5"`,
		"memory: 512Mi",
		"tcpSocket:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered YAML missing %q\n---\n%s", want, out)
		}
	}
	if strings.Contains(out, "env:") {
		t.Errorf("generic target should have no env block\n%s", out)
	}
	assertValidManifest(t, out, 2)
}

// TestRenderDefaults covers zero-value edge cases: no nodes and no image fall
// back to a single replica and the hook's default image.
func TestRenderDefaults(t *testing.T) {
	m := buildModel(domain.Topology{Name: "r", Target: "redis"})
	if m.Replicas != 1 {
		t.Fatalf("replicas = %d, want 1 (defaulted)", m.Replicas)
	}
	if m.Image != "redis" {
		t.Errorf("image = %q, want redis (hook default)", m.Image)
	}
	out, err := render(m)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out, `command: ["redis-cli", "ping"]`) {
		t.Errorf("redis readiness probe missing:\n%s", out)
	}
}

// TestRenderCassandraKafka spot-checks the remaining dedicated hooks render
// their expected ports.
func TestRenderCassandraKafka(t *testing.T) {
	tests := []struct {
		target   string
		wantPort string
	}{
		{"cassandra", "containerPort: 9042"},
		{"kafka", "containerPort: 9092"},
	}
	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			m := buildModel(domain.Topology{Name: tc.target, Target: tc.target, Nodes: 3})
			out, err := render(m)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if !strings.Contains(out, tc.wantPort) {
				t.Errorf("%s missing %q\n%s", tc.target, tc.wantPort, out)
			}
			assertValidManifest(t, out, 3)
		})
	}
}

// assertValidManifest does a lightweight structural check without a YAML
// dependency: exactly three documents (Namespace, Service, StatefulSet), the
// requested replica count, and no tab characters (which YAML forbids).
func assertValidManifest(t *testing.T, out string, wantReplicas int) {
	t.Helper()
	if strings.Contains(out, "\t") {
		t.Errorf("rendered YAML contains a tab character")
	}
	// Three kinds, one per document.
	for _, kind := range []string{
		"kind: Namespace", "kind: Service", "kind: StatefulSet",
	} {
		if strings.Count(out, kind) != 1 {
			t.Errorf("want exactly one %q document\n---\n%s", kind, out)
		}
	}
	// Documents are separated by a "---" line; three docs => two separators.
	if got := strings.Count(out, "\n---\n"); got != 2 {
		t.Errorf("document separators = %d, want 2", got)
	}
	if !strings.Contains(out, "replicas: "+itoa(wantReplicas)) {
		t.Errorf("want replicas: %d\n---\n%s", wantReplicas, out)
	}
}

// itoa is a tiny int-to-string helper kept local to the test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
