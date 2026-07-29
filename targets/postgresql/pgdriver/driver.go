// Package pgdriver implements the proofload driver.Driver, driver.Conn, and
// driver.ClusterAware capabilities for PostgreSQL using pgx/v5.
//
// Data model: proofload_kv(k BIGINT PRIMARY KEY, v BYTEA NOT NULL, seq BIGINT).
// Operations are mapped to prepared statements (see sql.go): read/r → SELECT,
// insert/w → upsert, update → UPDATE, scan → ranged SELECT.
//
// Consistency handling: driver.Config.Consistency selects a transaction
// isolation level ("read-committed"|"repeatable-read"|"serializable"). Because
// each Execute runs exactly one statement, PostgreSQL wraps it in an implicit
// transaction; setting default_transaction_isolation on the session (see
// Conn.applyConsistency) therefore governs every operation without an explicit
// BEGIN/COMMIT round-trip.
package pgdriver

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// pgDriver is the PostgreSQL driver.Driver implementation.
type pgDriver struct{}

// New returns a PostgreSQL driver. The engine's main package registers it.
func New() driver.Driver { return &pgDriver{} }

// Name implements driver.Driver.
func (*pgDriver) Name() string { return "postgresql" }

// Schema applies the idempotent DDL for the proofload_kv table. It opens a
// short-lived connection to the first endpoint, runs CREATE TABLE IF NOT EXISTS,
// and closes it. It is safe to call repeatedly.
func (d *pgDriver) Schema(ctx context.Context, cfg driver.Config, _ domain.Workload) error {
	conn, err := d.open(ctx, cfg, firstEndpoint(cfg.Endpoints))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("pgdriver: apply schema: %w", err)
	}
	return nil
}

// Connect opens one connection/session and applies the run's isolation level.
// The runner opens many such connections to reach the requested concurrency, so
// this deliberately returns a single *pgx.Conn rather than a pool.
func (d *pgDriver) Connect(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.open(ctx, cfg, firstEndpoint(cfg.Endpoints))
	if err != nil {
		return nil, err
	}
	c := &pgConn{conn: conn, scanLimit: defaultScanLimit}
	if err := c.applyConsistency(ctx, cfg.Consistency); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return c, nil
}

// open dials one endpoint using options resolved from cfg.Params + environment.
func (d *pgDriver) open(ctx context.Context, cfg driver.Config, endpoint string) (*pgx.Conn, error) {
	dsn := buildDSN(endpoint, resolveOptions(cfg.Params))
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: connect: %w", err)
	}
	return conn, nil
}

// firstEndpoint returns the first endpoint, or a localhost default when none are
// configured (chiefly for local development and tests).
func firstEndpoint(endpoints []string) string {
	if len(endpoints) == 0 {
		return "localhost:" + defaultPGPort
	}
	return endpoints[0]
}
