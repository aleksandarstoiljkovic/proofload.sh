// Package redisdriver implements the proofload driver.Driver, driver.Conn, and
// driver.ClusterAware capabilities for Redis using redis/go-redis/v9.
//
// Data model: string keys formatted as "proofload:{<k>}". The braces are a Redis
// Cluster hash tag so that, in cluster mode, keys sharing a tag map to the same
// slot; in standalone mode they are inert. Operations are mapped to commands (see
// cmd.go): read/r → GET, insert/w and update → SET, scan → MGET of N consecutive
// keys (N from workload params scan_limit, default 100).
//
// Consistency handling: Redis replication is asynchronous by default. The empty
// level and "none" issue no extra round-trips. Levels "wait"/"waitN" issue a
// WAIT after each write, blocking until the requested number of replicas
// acknowledge (or a short timeout elapses), giving a tunable durability floor.
package redisdriver

import (
	"context"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/redis/go-redis/v9"
)

// redisDriver is the Redis driver.Driver implementation.
type redisDriver struct{}

// New returns a Redis driver. The engine's main package registers it.
func New() driver.Driver { return &redisDriver{} }

// Name implements driver.Driver.
func (*redisDriver) Name() string { return "redis" }

// Schema is a no-op for Redis: as a schemaless key/value store it needs no DDL,
// topic, or keyspace creation before load. It is safe to call repeatedly.
func (*redisDriver) Schema(_ context.Context, _ driver.Config, _ domain.Workload) error {
	return nil
}

// Connect opens one client/session and resolves the run's consistency level. The
// runner opens many such connections to reach the requested concurrency, so this
// deliberately returns a single *redis.Client (one connection) rather than
// sharing a pool.
func (d *redisDriver) Connect(_ context.Context, cfg driver.Config) (driver.Conn, error) {
	cons, err := parseConsistency(cfg.Consistency, cfg.Params)
	if err != nil {
		return nil, err
	}
	client := d.open(cfg, firstEndpoint(cfg.Endpoints))
	return &redisConn{client: client, scanLimit: defaultScanLimit, cons: cons}, nil
}

// open constructs a single-connection client for one endpoint using options
// resolved from cfg.Params and the environment.
func (*redisDriver) open(cfg driver.Config, endpoint string) *redis.Client {
	return redis.NewClient(optionsFor(resolveOptions(endpoint, cfg.Params)))
}

// optionsFor renders resolved clientOptions into go-redis options for a single
// connection (PoolSize 1 keeps the runner in control of concurrency).
func optionsFor(o clientOptions) *redis.Options {
	return &redis.Options{
		Addr:     o.Addr,
		Password: o.Password,
		DB:       o.DB,
		PoolSize: 1,
	}
}

// firstEndpoint returns the first endpoint, or a localhost default when none are
// configured (chiefly for local development and tests).
func firstEndpoint(endpoints []string) string {
	if len(endpoints) == 0 {
		return "localhost:" + defaultRedisPort
	}
	return endpoints[0]
}
