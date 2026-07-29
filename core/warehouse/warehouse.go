// Package warehouse is an embedded DuckDB store for cross-run analytics. It
// ingests the headline facts of each proofload run (from its manifest, summary,
// and optional verification report, read via the emit package) into a single
// runs table keyed by run_id, so many runs can be compared and trended over time.
//
// The store is opened as a plain file through database/sql using the DuckDB
// driver (github.com/marcboeker/go-duckdb/v2, registered as "duckdb"). It imports
// only core/domain, core/emit, the Go standard library, and that driver.
package warehouse

import (
	"database/sql"

	_ "github.com/marcboeker/go-duckdb/v2" // registers the "duckdb" sql driver
)

// Warehouse is a handle to an embedded DuckDB analytics database.
type Warehouse struct {
	db *sql.DB
}

// schema is the DDL for the single runs table. It is created on Open if absent.
const schema = `
CREATE TABLE IF NOT EXISTS runs (
	run_id         VARCHAR PRIMARY KEY,
	target         VARCHAR NOT NULL,
	workload       VARCHAR NOT NULL,
	mode           VARCHAR,
	created_at     TIMESTAMP,
	throughput     DOUBLE,
	p50            DOUBLE,
	p99            DOUBLE,
	p999           DOUBLE,
	max            DOUBLE,
	total          BIGINT,
	errors         BIGINT,
	client_bound   BOOLEAN,
	verify_verdict VARCHAR
);`

// Open opens (or creates) the DuckDB database at path and ensures the schema
// exists. Pass ":memory:" for an ephemeral in-process database.
func Open(path string) (*Warehouse, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Warehouse{db: db}, nil
}

// Close releases the underlying database connection.
func (w *Warehouse) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	return w.db.Close()
}
