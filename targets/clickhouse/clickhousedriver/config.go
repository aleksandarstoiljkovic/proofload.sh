package clickhousedriver

import (
	"fmt"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/proofload/proofload/core/driver"
)

// Defaults applied when neither params nor environment supply a value. 9000 is
// the ClickHouse native-protocol port; "default" is the built-in database/user.
const (
	defaultCHPort   = "9000"
	defaultDatabase = "default"
	defaultUser     = "default"
)

// supportedConsistency lists the levels this target exposes, ordered weakest to
// strongest. Kept as a package var so consistencySettings, ConsistencyLevels and
// tests share one definition.
var supportedConsistency = []string{"none", "quorum"}

// consistencySettings maps a driver.Config.Consistency string to the ClickHouse
// session settings applied for the whole run. It is pure so the mapping is
// unit-testable. "none"/"" leaves defaults (returns nil). "quorum" requires a
// two-replica insert quorum and forces sequentially consistent reads, so a write
// acknowledged under "quorum" is durable on both replicas and visible to a later
// quorum read. Unknown levels are rejected so misconfiguration fails fast.
func consistencySettings(consistency string) (clickhouse.Settings, error) {
	switch consistency {
	case "", "none":
		return nil, nil
	case "quorum":
		return clickhouse.Settings{
			"insert_quorum":                 2,
			"select_sequential_consistency": 1,
		}, nil
	default:
		return nil, fmt.Errorf("clickhousedriver: unsupported consistency %q", consistency)
	}
}

// isClustered reports whether the target is a replicated cluster rather than a
// single standalone server. It is true when the resolved cluster carries more
// than one node, or when params["cluster"] is present (an explicit override for
// pointing at one endpoint of an existing cluster).
func isClustered(cfg driver.Config) bool {
	if len(cfg.Cluster.Nodes) > 1 {
		return true
	}
	_, ok := cfg.Params["cluster"]
	return ok
}

// firstEndpoint returns the first endpoint, or a localhost default when none are
// configured (chiefly for local development and tests).
func firstEndpoint(endpoints []string) string {
	if len(endpoints) == 0 {
		return "localhost:" + defaultCHPort
	}
	return endpoints[0]
}

// normalizeAddr renders an endpoint as a host:port native-protocol address,
// defaulting the port to 9000. It is pure (no environment access) so address
// building is unit-testable.
func normalizeAddr(endpoint string) string {
	host, port := splitHostPort(strings.TrimSpace(endpoint))
	return host + ":" + port
}

// splitHostPort splits a "host:port" endpoint, defaulting the port to 9000 when
// absent. It splits on the last colon so IPv6 literals in brackets survive.
func splitHostPort(endpoint string) (host, port string) {
	if endpoint == "" {
		return "localhost", defaultCHPort
	}
	if i := strings.LastIndex(endpoint, ":"); i >= 0 && !strings.HasSuffix(endpoint, "]") {
		return endpoint[:i], endpoint[i+1:]
	}
	return endpoint, defaultCHPort
}

// resolveAuth derives the ClickHouse Auth from config params and the
// environment. The database comes from params (dbname/database) or the built-in
// "default"; the username from CLICKHOUSE_USER or "default". The password is only
// ever read from the environment (PROOFLOAD_CLICKHOUSE_PASSWORD, then
// CLICKHOUSE_PASSWORD) so it never has to appear in checked-in config.
func resolveAuth(params map[string]any) clickhouse.Auth {
	return clickhouse.Auth{
		Database: firstNonEmpty(paramString(params, "dbname"), paramString(params, "database"), defaultDatabase),
		Username: firstNonEmpty(os.Getenv("CLICKHOUSE_USER"), defaultUser),
		Password: firstNonEmpty(os.Getenv("PROOFLOAD_CLICKHOUSE_PASSWORD"), os.Getenv("CLICKHOUSE_PASSWORD")),
	}
}

// buildOptions assembles native-protocol clickhouse.Options for one endpoint:
// address, resolved auth, and the run's consistency settings. MaxOpen/IdleConns
// are pinned to 1 so each Connect owns exactly one connection — the runner opens
// many to reach the requested concurrency.
func buildOptions(cfg driver.Config, endpoint string) (*clickhouse.Options, error) {
	settings, err := consistencySettings(cfg.Consistency)
	if err != nil {
		return nil, err
	}
	return &clickhouse.Options{
		Protocol:     clickhouse.Native,
		Addr:         []string{normalizeAddr(endpoint)},
		Auth:         resolveAuth(cfg.Params),
		Settings:     settings,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}, nil
}

// paramString reads a string-valued param, tolerating a nil map.
func paramString(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	if s, ok := params[key].(string); ok {
		return s
	}
	return ""
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
