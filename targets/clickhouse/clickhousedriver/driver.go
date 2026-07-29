// Package clickhousedriver implements the proofload driver.Driver, driver.Conn,
// and driver.ClusterAware capabilities for ClickHouse using clickhouse-go/v2 over
// the native protocol.
//
// Data model: proofload_kv(k Int64, v String, seq Int64). Writes are append-only
// INSERTs; the engine is ReplacingMergeTree(seq) standalone, or
// ReplicatedReplacingMergeTree(...) with seq as the version column when
// clustered. Reads pick the greatest seq per key, so the latest write is visible
// immediately regardless of background merges (see sql.go).
//
// Operations map to SQL (see planFor): read/r → SELECT ... ORDER BY seq DESC
// LIMIT 1, insert/w/update → INSERT, scan → ranged SELECT.
//
// Consistency handling: driver.Config.Consistency selects ClickHouse session
// settings applied to the whole connection (see consistencySettings): "none"
// uses defaults; "quorum" sets insert_quorum=2 and select_sequential_consistency
// so an acknowledged write is durable on both replicas before it is read back.
package clickhousedriver

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// chDriver is the ClickHouse driver.Driver implementation.
type chDriver struct{}

// New returns a ClickHouse driver. The engine's main package registers it.
func New() driver.Driver { return &chDriver{} }

// Name implements driver.Driver.
func (*chDriver) Name() string { return "clickhouse" }

// Schema applies the idempotent DDL for proofload_kv. It opens a short-lived
// connection to the first endpoint and runs CREATE TABLE IF NOT EXISTS, choosing
// the replicated ON CLUSTER engine when the target is clustered and the plain
// standalone engine otherwise. It is safe to call repeatedly.
func (d *chDriver) Schema(ctx context.Context, cfg driver.Config, _ domain.Workload) error {
	conn, err := d.open(ctx, cfg, firstEndpoint(cfg.Endpoints))
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Exec(ctx, createTableSQL(isClustered(cfg))); err != nil {
		return fmt.Errorf("clickhousedriver: apply schema: %w", err)
	}
	return nil
}

// Connect opens one connection/session and applies the run's consistency
// settings (via buildOptions). The runner opens many such connections to reach
// the requested concurrency, so options pin the pool to a single connection.
func (d *chDriver) Connect(ctx context.Context, cfg driver.Config) (driver.Conn, error) {
	conn, err := d.open(ctx, cfg, firstEndpoint(cfg.Endpoints))
	if err != nil {
		return nil, err
	}
	return &chConn{conn: conn, scanLimit: defaultScanLimit}, nil
}

// open dials one endpoint with options resolved from cfg.Params + environment,
// then pings so a failure surfaces at Connect/Schema rather than mid-run.
func (d *chDriver) open(ctx context.Context, cfg driver.Config, endpoint string) (chdriver.Conn, error) {
	opts, err := buildOptions(cfg, endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: open %s: %w", endpoint, err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhousedriver: ping %s: %w", endpoint, err)
	}
	return conn, nil
}
