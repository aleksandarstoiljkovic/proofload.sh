package cassandradriver

import (
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/driver"
)

// defaultHost is dialed when a config carries no endpoints (local development
// and unit contexts). gocql's default native-protocol port is 9042.
const defaultHost = "localhost:9042"

// clusterOptions carries the non-endpoint connection parameters resolved from
// driver.Config.Params and the environment.
type clusterOptions struct {
	Keyspace    string
	Consistency gocql.Consistency
	User        string
	Password    string
	Timeout     time.Duration
}

// hostsFrom returns the endpoint list, or a single localhost default when none
// are configured. gocql keeps any host:port already present on each entry.
func hostsFrom(endpoints []string) []string {
	if len(endpoints) == 0 {
		return []string{defaultHost}
	}
	return endpoints
}

// resolveReplicationFactor returns the keyspace replication factor: the
// cluster's configured value, clamped to a minimum of 1 when unset.
func resolveReplicationFactor(cfg driver.Config) int {
	if rf := cfg.Cluster.ReplicationFactor; rf > 0 {
		return rf
	}
	return 1
}

// resolveKeyspace returns params.keyspace when set, else the fixed default.
func resolveKeyspace(cfg driver.Config) string {
	if ks := paramString(cfg.Params, "keyspace"); ks != "" {
		return ks
	}
	return keyspaceName
}

// resolveOptions derives clusterOptions from config params and the environment.
// The password is only ever read from the environment
// (PROOFLOAD_CASSANDRA_PASSWORD) so it never appears in checked-in config. A
// consistency error is surfaced here so Connect/Schema fail fast.
func resolveOptions(cfg driver.Config, keyspace string) (clusterOptions, error) {
	cons, err := consistencyLevel(cfg.Consistency)
	if err != nil {
		return clusterOptions{}, err
	}
	return clusterOptions{
		Keyspace:    keyspace,
		Consistency: cons,
		User:        firstNonEmpty(paramString(cfg.Params, "user"), os.Getenv("PROOFLOAD_CASSANDRA_USER")),
		Password:    os.Getenv("PROOFLOAD_CASSANDRA_PASSWORD"),
		Timeout:     resolveTimeout(cfg.Params),
	}, nil
}

// resolveTimeout reads params.timeout_ms, defaulting to 10s.
func resolveTimeout(params map[string]any) time.Duration {
	if ms := asInt(paramAny(params, "timeout_ms")); ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return 10 * time.Second
}

// buildCluster assembles a gocql.ClusterConfig for the given hosts and options.
// Keyspace may be empty (Schema opens a keyspace-less session to create it).
func buildCluster(hosts []string, o clusterOptions) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = o.Keyspace
	cluster.Consistency = o.Consistency
	cluster.Timeout = o.Timeout
	cluster.ConnectTimeout = o.Timeout
	cluster.NumConns = 1 // the runner opens many sessions for concurrency
	// Retry a failed query on another coordinator so a single downed node is
	// tolerated transparently: with RF=3 at QUORUM, 2 of 3 replicas remain, so a
	// retry routed to a live node still satisfies the consistency level instead
	// of surfacing the dead coordinator as an error.
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	// Use only the provided host:port endpoints; do NOT query system.peers for
	// other nodes. A dockerized cluster advertises its internal container IPs
	// (e.g. 172.x:9042), which are unreachable from the host through the
	// published port mappings. This is the standard gocql workaround for a
	// cluster reached via NAT / port-forwarding. gocql honors the per-host port
	// on each provided endpoint, so the three published ports route correctly.
	cluster.DisableInitialHostLookup = true
	if o.User != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: o.User,
			Password: o.Password,
		}
	}
	return cluster
}

// paramString reads a string-valued param, tolerating a nil map.
func paramString(params map[string]any, key string) string {
	if s, ok := paramAny(params, key).(string); ok {
		return s
	}
	return ""
}

// paramAny reads a param value, tolerating a nil map.
func paramAny(params map[string]any, key string) any {
	if params == nil {
		return nil
	}
	return params[key]
}

// firstNonEmpty returns the first non-empty argument, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
