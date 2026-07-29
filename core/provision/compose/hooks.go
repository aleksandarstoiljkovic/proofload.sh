package compose

// targetHook carries the per-target knobs that turn the generic N-node bring-up
// into something sensible for a specific database. Unknown targets use
// genericHook, which still yields a working multi-node project.
type targetHook struct {
	known         bool
	defaultImage  string
	containerPort int
	basePort      int // first published host port; node i gets basePort+i
	env           []kv
	health        *health
	volumePath    string // container path to persist as a named volume; "" = none
}

// genericHook drives the fallback path for targets without a dedicated hook.
// It exposes a single port per node and persists no data.
var genericHook = targetHook{
	known:         false,
	containerPort: 8080,
	basePort:      18080,
}

// hooks holds the dedicated per-target configurations.
var hooks = map[string]targetHook{
	"postgresql": {
		known:         true,
		defaultImage:  "postgres",
		containerPort: 5432,
		basePort:      15432,
		env: []kv{
			{K: "POSTGRES_PASSWORD", V: "proofload"},
			{K: "POSTGRES_USER", V: "proofload"},
			{K: "POSTGRES_DB", V: "proofload"},
		},
		health: &health{
			Test:        []string{"CMD-SHELL", "pg_isready -U proofload"},
			Interval:    "5s",
			Timeout:     "5s",
			Retries:     10,
			StartPeriod: "10s",
		},
		volumePath: "/var/lib/postgresql/data",
	},
	"redis": {
		known:         true,
		defaultImage:  "redis",
		containerPort: 6379,
		basePort:      16379,
		health: &health{
			Test:        []string{"CMD", "redis-cli", "ping"},
			Interval:    "5s",
			Timeout:     "3s",
			Retries:     10,
			StartPeriod: "5s",
		},
		volumePath: "/data",
	},
}

// hookFor returns the dedicated hook for a target, or the generic fallback.
//
// TODO: clustered replication topologies (e.g. PostgreSQL streaming replicas
// wired via primary_conninfo, or a Redis primary/replica REPLICAOF chain) are
// not yet rendered. Every node currently boots as an independent instance; the
// primary/replica roles are advisory metadata only.
func hookFor(target string) targetHook {
	if h, ok := hooks[target]; ok {
		return h
	}
	return genericHook
}
