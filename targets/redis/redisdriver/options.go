package redisdriver

import (
	"os"
	"strings"
)

// defaultRedisPort is applied when an endpoint omits a port.
const defaultRedisPort = "6379"

// clientOptions carries the connection parameters derived from an endpoint plus
// the environment, ready to be handed to go-redis.
type clientOptions struct {
	Addr     string
	Password string
	DB       int
}

// resolveOptions derives clientOptions from one endpoint and config params. The
// address is normalized to host:port; the password is only ever taken from the
// environment (PROOFLOAD_REDIS_PASSWORD, then REDIS_PASSWORD) so it never has to
// appear in checked-in config; DB is fixed to 0.
//
// The params argument is accepted for symmetry with other targets and future
// knobs; address and password resolution do not currently read from it.
func resolveOptions(endpoint string, _ map[string]any) clientOptions {
	return clientOptions{
		Addr:     normalizeAddr(endpoint),
		Password: firstNonEmpty(os.Getenv("PROOFLOAD_REDIS_PASSWORD"), os.Getenv("REDIS_PASSWORD")),
		DB:       0,
	}
}

// normalizeAddr canonicalizes an endpoint to host:port. A bare host gains the
// default port; an empty endpoint becomes localhost:6379. It splits on the last
// colon so IPv6 literals in brackets survive.
func normalizeAddr(endpoint string) string {
	host, port := splitHostPort(endpoint)
	return host + ":" + port
}

// splitHostPort splits a "host:port" endpoint, defaulting the port to 6379 when
// absent. It splits on the last colon so bracketed IPv6 literals are preserved.
func splitHostPort(endpoint string) (host, port string) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "localhost", defaultRedisPort
	}
	if i := strings.LastIndex(endpoint, ":"); i >= 0 && !strings.HasSuffix(endpoint, "]") {
		return endpoint[:i], endpoint[i+1:]
	}
	return endpoint, defaultRedisPort
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
