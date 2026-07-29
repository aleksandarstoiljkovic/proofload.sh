package k8s

// targetHook carries the per-target knobs that turn the generic N-node
// StatefulSet into something sensible for a specific database. Unknown targets
// use genericHook, which still yields a working multi-node StatefulSet.
type targetHook struct {
	known         bool
	defaultImage  string
	portName      string
	containerPort int
	env           []kv
	readiness     *probe
}

// genericHook drives the fallback path for targets without a dedicated hook.
// It exposes a single port per pod and adds a TCP readiness probe on it.
var genericHook = targetHook{
	known:         false,
	portName:      "tcp",
	containerPort: 8080,
	readiness: &probe{
		TCPPort:      8080,
		InitialDelay: 5,
		Period:       5,
	},
}

// hooks holds the dedicated per-target configurations.
var hooks = map[string]targetHook{
	"postgresql": {
		known:         true,
		defaultImage:  "postgres",
		portName:      "postgres",
		containerPort: 5432,
		env: []kv{
			{K: "POSTGRES_PASSWORD", V: "proofload"},
			{K: "POSTGRES_USER", V: "proofload"},
			{K: "POSTGRES_DB", V: "proofload"},
		},
		readiness: &probe{
			Exec:         []string{"pg_isready", "-U", "proofload"},
			InitialDelay: 10,
			Period:       5,
		},
	},
	"redis": {
		known:         true,
		defaultImage:  "redis",
		portName:      "redis",
		containerPort: 6379,
		readiness: &probe{
			Exec:         []string{"redis-cli", "ping"},
			InitialDelay: 5,
			Period:       5,
		},
	},
	"cassandra": {
		known:         true,
		defaultImage:  "cassandra",
		portName:      "cql",
		containerPort: 9042,
		readiness: &probe{
			Exec:         []string{"cqlsh", "-e", "describe cluster"},
			InitialDelay: 30,
			Period:       10,
		},
	},
	"kafka": {
		known:         true,
		defaultImage:  "kafka",
		portName:      "kafka",
		containerPort: 9092,
		readiness: &probe{
			TCPPort:      9092,
			InitialDelay: 15,
			Period:       10,
		},
	},
}

// hookFor returns the dedicated hook for a target, or the generic fallback.
//
// TODO: clustered replication topologies (e.g. PostgreSQL streaming replicas,
// a Redis primary/replica REPLICAOF chain, Cassandra seed lists, or a Kafka
// KRaft quorum) are not yet rendered. Every pod currently boots as an
// independent instance; the primary/replica roles are advisory metadata only,
// exactly as in the compose backend.
func hookFor(target string) targetHook {
	if h, ok := hooks[target]; ok {
		return h
	}
	return genericHook
}
