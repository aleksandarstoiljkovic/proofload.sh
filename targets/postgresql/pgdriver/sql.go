package pgdriver

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/proofload/proofload/core/domain"
)

// Prepared-statement names. pgx invokes a prepared statement when the SQL string
// passed to Query/Exec matches a registered statement name, so these constants
// are used both to prepare (Conn.Prepare) and to invoke (Conn.Execute).
const (
	stmtRead   = "pl_read"
	stmtUpsert = "pl_upsert"
	stmtUpdate = "pl_update"
	stmtScan   = "pl_scan"
)

// defaultScanLimit is used when a workload does not set params.scan_limit.
const defaultScanLimit = 100

// statements returns the prepared-statement name→SQL map for the hot queries.
// It is the single source of truth for both preparation and testing.
func statements() map[string]string {
	return map[string]string{
		stmtRead:   `SELECT v FROM proofload_kv WHERE k=$1`,
		stmtUpsert: `INSERT INTO proofload_kv (k, v, seq) VALUES ($1, $2, $3) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, seq = EXCLUDED.seq`,
		stmtUpdate: `UPDATE proofload_kv SET v = $2, seq = $3 WHERE k = $1`,
		stmtScan:   `SELECT k, v FROM proofload_kv WHERE k >= $1 ORDER BY k LIMIT $2`,
	}
}

// opPlan is the resolved execution plan for one operation: which prepared
// statement to run, the positional arguments, and how to interpret the result.
type opPlan struct {
	stmt string
	args []any
	kind opKind
}

// opKind classifies how Execute reads back the result of a statement.
type opKind int

const (
	kindRead  opKind = iota // single-row read into Observed ([]byte)
	kindWrite               // no rows read back; Rows = affected
	kindScan                // multi-row read into Observed ([][]byte)
)

// planFor maps a domain.Operation onto a prepared statement and its arguments.
// It is a pure function so the op→SQL selection and argument wiring are unit
// testable without a database. scanLimit is resolved from the workload once.
func planFor(op domain.Operation, scanLimit int) (opPlan, error) {
	switch op.Type {
	case "read", "r":
		return opPlan{stmt: stmtRead, args: []any{op.Key}, kind: kindRead}, nil
	case "insert", "w":
		return opPlan{stmt: stmtUpsert, args: []any{op.Key, op.Value, op.Seq}, kind: kindWrite}, nil
	case "update":
		return opPlan{stmt: stmtUpdate, args: []any{op.Key, op.Value, op.Seq}, kind: kindWrite}, nil
	case "scan":
		if scanLimit <= 0 {
			scanLimit = defaultScanLimit
		}
		return opPlan{stmt: stmtScan, args: []any{op.Key, scanLimit}, kind: kindScan}, nil
	default:
		return opPlan{}, fmt.Errorf("pgdriver: unsupported op type %q", op.Type)
	}
}

// isoLevel maps a driver.Config.Consistency string to a pgx transaction
// isolation level. The empty string defaults to read committed (Postgres's own
// default). Unknown levels are rejected so misconfiguration fails fast.
func isoLevel(consistency string) (pgx.TxIsoLevel, error) {
	switch consistency {
	case "", "read-committed", "read committed":
		return pgx.ReadCommitted, nil
	case "repeatable-read", "repeatable read":
		return pgx.RepeatableRead, nil
	case "serializable":
		return pgx.Serializable, nil
	default:
		return "", fmt.Errorf("pgdriver: unsupported consistency %q", consistency)
	}
}

// scanLimitFromWorkload reads params.scan_limit (or params.limit) from a
// workload, falling back to defaultScanLimit. Values arrive from YAML as int or
// float64 depending on the parser, so both are accepted.
func scanLimitFromWorkload(w domain.Workload) int {
	for _, key := range []string{"scan_limit", "limit"} {
		if v, ok := w.Params[key]; ok {
			if n := asInt(v); n > 0 {
				return n
			}
		}
	}
	return defaultScanLimit
}

// asInt coerces a YAML-decoded numeric value to int, returning 0 when it is not
// a recognised numeric type.
func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
