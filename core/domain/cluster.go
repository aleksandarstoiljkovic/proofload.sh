package domain

// NodeRole describes a node's function in a cluster topology.
type NodeRole string

const (
	RolePrimary NodeRole = "primary"
	RoleReplica NodeRole = "replica"
	RoleSeed    NodeRole = "seed"
	RoleBroker  NodeRole = "broker"
	RoleGeneric NodeRole = "node"
)

// ControlMethod is how proofload reaches a node's control plane to inject faults
// or read a single replica directly.
type ControlMethod string

const (
	ControlK8s    ControlMethod = "k8s"
	ControlDocker ControlMethod = "docker"
	ControlSSH    ControlMethod = "ssh"
	ControlNone   ControlMethod = "none" // remote/managed: no control, perf only
)

// ControlEndpoint locates a node's control plane. Ref is method-specific: a pod
// name (k8s), container name (docker), or host (ssh).
type ControlEndpoint struct {
	Method    ControlMethod     `json:"method"`
	Ref       string            `json:"ref"`
	Namespace string            `json:"namespace,omitempty"`
	Extra     map[string]string `json:"extra,omitempty"`
}

// Node is a resolved, running cluster member: a client-facing endpoint plus a
// control endpoint used by verification and fault injection.
type Node struct {
	ID      string          `json:"id"`
	Role    NodeRole        `json:"role"`
	Client  string          `json:"client"` // host:port for the load engine
	Control ControlEndpoint `json:"control"`
}

// ClusterSpec is the resolved runtime view of a cluster: the nodes a run should
// talk to, the replication factor, and the consistency levels to exercise.
type ClusterSpec struct {
	Nodes             []Node   `json:"nodes"`
	ReplicationFactor int      `json:"replication_factor"`
	Consistency       []string `json:"consistency,omitempty"`
}

// Backend selects who owns the cluster's lifecycle.
type Backend string

const (
	BackendKubernetes Backend = "kubernetes" // primary: chaos-capable, uniform local/cloud
	BackendCompose    Backend = "compose"    // fast local dev loop
	BackendExternal   Backend = "external"   // remote/managed: proofload only connects
)

// ResourceSpec bounds per-node resources so runs are comparable across machines.
type ResourceSpec struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// Topology is the declarative input to a Provisioner (parsed from topology.yaml).
// It describes the desired cluster; the provisioner returns a resolved
// ClusterSpec once the topology is up.
type Topology struct {
	Name              string       `json:"name"`
	Backend           Backend      `json:"backend"`
	Target            string       `json:"target"`
	Image             string       `json:"image,omitempty"`
	Version           string       `json:"version,omitempty"`
	Nodes             int          `json:"nodes"`
	ReplicationFactor int          `json:"replication_factor"`
	Resources         ResourceSpec `json:"resources,omitempty"`
	// Bundle, when set, is the path to a directory holding a target-supplied
	// docker-compose.yml plus a proofload-cluster.json manifest. A provisioner
	// that supports bundles runs it verbatim instead of rendering a generic
	// topology, so complex multi-component clusters (e.g. ClickHouse + Keeper)
	// live as real compose files in the target directory.
	Bundle string         `json:"bundle,omitempty"`
	Extra  map[string]any `json:"extra,omitempty"`
}
