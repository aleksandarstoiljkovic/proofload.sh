// Package k8s implements the provision.Provisioner port on top of the `kubectl`
// CLI. It renders a deterministic set of manifests for a domain.Topology (a
// Namespace, a headless Service, and a StatefulSet with one pod per node),
// applies them, waits for the rollout, and resolves each pod into a
// domain.ClusterSpec whose nodes carry Kubernetes control endpoints for later
// fault injection (pod-kill, NetworkPolicy).
//
// This is proofload's primary backend: because pods are individually
// addressable and NetworkPolicies can partition them, it gives real
// chaos-capable fault injection that the Compose backend cannot.
//
// It shells out to `kubectl` via os/exec and hand-renders YAML with
// text/template so it needs no client-go or other third-party dependency,
// mirroring how the compose backend shells out to `docker`.
//
// Client endpoints use in-cluster pod DNS
// ("<pod>.<svc>.<ns>.svc.cluster.local:<port>"), so the load engine is assumed
// to run inside the cluster. Out-of-cluster access via `kubectl port-forward`
// is a documented TODO (see resolveClient).
package k8s

import (
	"fmt"
	"sort"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// kv is a single, order-stable environment entry.
type kv struct{ K, V string }

// probe is a container readinessProbe rendered into the StatefulSet. Exactly
// one of Exec or TCPPort is used: an exec probe when Exec is non-empty,
// otherwise a tcpSocket probe on TCPPort.
type probe struct {
	Exec         []string
	TCPPort      int
	InitialDelay int
	Period       int
}

// manifestModel is the full rendered set of manifests for one topology. It is
// the single source of truth shared by rendering, bring-up, and node
// resolution so the pod names it emits always match the pods kubectl creates.
type manifestModel struct {
	Name          string // StatefulSet + headless Service name = t.Name
	Namespace     string // proofload-<t.Name>
	Container     string // container name
	Image         string
	Replicas      int
	PortName      string
	ContainerPort int
	Env           []kv
	Readiness     *probe
	CPULimit      string
	MemLimit      string
	Roles         []domain.NodeRole // per-ordinal node role
}

// namespaceFor returns the Kubernetes namespace for a topology.
func namespaceFor(t domain.Topology) string { return "proofload-" + t.Name }

// buildModel turns a Topology into a deterministic manifestModel.
func buildModel(t domain.Topology) manifestModel {
	hook := hookFor(t.Target)
	n := t.Nodes
	if n <= 0 {
		n = 1
	}
	m := manifestModel{
		Name:          t.Name,
		Namespace:     namespaceFor(t),
		Container:     t.Name,
		Image:         resolveImage(t, hook),
		Replicas:      n,
		PortName:      hook.portName,
		ContainerPort: hook.containerPort,
		Env:           sortedEnv(hook.env),
		Readiness:     hook.readiness,
		CPULimit:      t.Resources.CPU,
		MemLimit:      t.Resources.Memory,
	}
	for i := 0; i < n; i++ {
		m.Roles = append(m.Roles, roleFor(hook, i))
	}
	return m
}

// podName returns the StatefulSet-assigned name of the i-th pod.
func (m manifestModel) podName(i int) string {
	return fmt.Sprintf("%s-%d", m.Name, i)
}

// resolveClient returns the in-cluster DNS client endpoint for a pod.
//
// TODO: out-of-cluster runs need a `kubectl port-forward` helper that maps each
// pod to a localhost port and rewrites these endpoints accordingly. Until then
// the load engine is assumed to run inside the cluster.
func (m manifestModel) resolveClient(pod string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local:%d",
		pod, m.Name, m.Namespace, m.ContainerPort)
}

// controlFor builds the Kubernetes control endpoint for a pod so the nemesis
// can `kubectl delete pod` or apply NetworkPolicies against it.
func (m manifestModel) controlFor(pod string) domain.ControlEndpoint {
	return domain.ControlEndpoint{
		Method:    domain.ControlK8s,
		Ref:       pod,
		Namespace: m.Namespace,
	}
}

// roleAt returns the role for the ordinal-th pod, defaulting to generic for
// ordinals outside the modelled range.
func (m manifestModel) roleAt(ordinal int) domain.NodeRole {
	if ordinal >= 0 && ordinal < len(m.Roles) {
		return m.Roles[ordinal]
	}
	return domain.RoleGeneric
}

// nodes resolves the model into the deterministic set of cluster nodes.
func (m manifestModel) nodes() []domain.Node {
	out := make([]domain.Node, 0, m.Replicas)
	for i := 0; i < m.Replicas; i++ {
		pod := m.podName(i)
		out = append(out, domain.Node{
			ID:      pod,
			Role:    m.roleAt(i),
			Client:  m.resolveClient(pod),
			Control: m.controlFor(pod),
		})
	}
	return out
}

// resolveImage prefers the topology's explicit image, falling back to the
// per-target default, and appends :version when set.
func resolveImage(t domain.Topology, hook targetHook) string {
	img := t.Image
	if img == "" {
		img = hook.defaultImage
	}
	if img == "" {
		img = t.Target // last resort: treat target as an image name
	}
	if t.Version != "" {
		return img + ":" + t.Version
	}
	return img
}

// roleFor assigns node 0 as primary and the rest as replicas for known
// replicating targets; unknown targets are generic nodes.
func roleFor(hook targetHook, i int) domain.NodeRole {
	if !hook.known {
		return domain.RoleGeneric
	}
	if i == 0 {
		return domain.RolePrimary
	}
	return domain.RoleReplica
}

// sortedEnv returns env entries in stable key order for deterministic output.
func sortedEnv(env []kv) []kv {
	if len(env) == 0 {
		return nil
	}
	out := append([]kv(nil), env...)
	sort.Slice(out, func(a, b int) bool { return out[a].K < out[b].K })
	return out
}

// render renders the manifestModel into a multi-document manifest YAML.
func render(m manifestModel) (string, error) {
	var b strings.Builder
	if err := manifestTmpl.Execute(&b, m); err != nil {
		return "", err
	}
	return b.String(), nil
}
