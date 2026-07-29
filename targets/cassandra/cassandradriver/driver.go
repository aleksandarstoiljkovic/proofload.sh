// Package cassandradriver implements the proofload driver.Driver, driver.Conn,
// and driver.ClusterAware capabilities for Apache Cassandra using gocql.
//
// Data model: keyspace "proofload" (SimpleStrategy, replication_factor from the
// cluster spec), table kv(k bigint PRIMARY KEY, v blob, seq bigint). Operations
// map to prepared/bound CQL (see cql.go): read/r → SELECT, insert/w → INSERT
// (CQL's blind write is an upsert), update → UPDATE, scan → token-range SELECT.
//
// Consistency handling: driver.Config.Consistency selects a gocql consistency
// level ("one"|"quorum"|"local_quorum"|"all", default QUORUM) applied on the
// cluster config so every operation on the session runs at that level.
package cassandradriver

import (
	"context"
	"fmt"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// cassDriver is the Cassandra driver.Driver implementation.
type cassDriver struct{}

// New returns a Cassandra driver. The engine's main package registers it.
func New() driver.Driver { return &cassDriver{} }

// Name implements driver.Driver.
func (*cassDriver) Name() string { return "cassandra" }

// Schema applies the idempotent keyspace and table DDL. It opens a short-lived,
// keyspace-less session (the keyspace may not exist yet), creates the keyspace
// with the cluster's replication factor, then the table. It is safe to call
// repeatedly; gocql waits for schema agreement across the ring on each DDL.
func (*cassDriver) Schema(ctx context.Context, cfg driver.Config, _ domain.Workload) error {
	keyspace := resolveKeyspace(cfg)
	opts, err := resolveOptions(cfg, "") // no keyspace: it is what we are creating
	if err != nil {
		return err
	}
	session, err := buildCluster(hostsFrom(cfg.Endpoints), opts).CreateSession()
	if err != nil {
		return fmt.Errorf("cassandradriver: open schema session: %w", err)
	}
	defer session.Close()

	rf := resolveReplicationFactor(cfg)
	if err := session.Query(createKeyspaceCQL(keyspace, rf)).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandradriver: create keyspace: %w", err)
	}
	if err := session.Query(createTableCQL(keyspace)).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandradriver: create table: %w", err)
	}
	return nil
}

// Connect opens one session bound to the proofload keyspace at the run's
// consistency level. The runner opens many such sessions to reach the requested
// concurrency, so this keeps a single connection per session (NumConns = 1).
func (*cassDriver) Connect(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	keyspace := resolveKeyspace(cfg)
	opts, err := resolveOptions(cfg, keyspace)
	if err != nil {
		return nil, err
	}
	session, err := buildCluster(hostsFrom(cfg.Endpoints), opts).CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandradriver: connect: %w", err)
	}
	return &cassConn{session: session, scanLimit: defaultScanLimit}, nil
}

// Ensure cassDriver satisfies both the required Driver port and the optional
// ClusterAware capability.
var (
	_ driver.Driver       = (*cassDriver)(nil)
	_ driver.ClusterAware = (*cassDriver)(nil)
	_ driver.Conn         = (*cassConn)(nil)
)
