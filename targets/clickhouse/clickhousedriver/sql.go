package clickhousedriver

import (
	"fmt"

	"github.com/proofload/proofload/core/domain"
)

// tableName is the fixed key/value table every operation touches.
const tableName = "proofload_kv"

// clusterName is the ClickHouse logical cluster referenced by ON CLUSTER DDL and
// implied by the ReplicatedReplacingMergeTree znode-path/replica macros. It is a
// constant because the cluster bundle (cluster/config/server-common.xml) defines
// exactly one logical cluster with this name.
const clusterName = "proofload"

// columnsDDL is the column list shared by the standalone and replicated DDL.
const columnsDDL = "(k Int64, v String, seq Int64)"

// defaultScanLimit is used when a workload does not set params.scan_limit.
const defaultScanLimit = 100

// Hot-path SQL. Positional "?" placeholders are bound client-side by
// clickhouse-go, so passing these strings to Exec/Query/QueryRow yields
// parameterised execution over the native protocol.
//
// readSQL selects the value with the highest seq for a key. Because writes are
// append-only INSERTs into a ReplacingMergeTree, several rows may coexist for a
// key until a background merge collapses them; ORDER BY seq DESC LIMIT 1 makes
// the read observe the latest write immediately, without depending on merges.
const (
	readSQL   = `SELECT v FROM proofload_kv WHERE k = ? ORDER BY seq DESC LIMIT 1`
	insertSQL = `INSERT INTO proofload_kv (k, v, seq) VALUES (?, ?, ?)`
	scanSQL   = `SELECT k, v FROM proofload_kv WHERE k >= ? ORDER BY k LIMIT ?`
)

// Logical statement names, used as the keys of statements() so tests can assert
// every hot operation has SQL without depending on the SQL text itself.
const (
	stmtRead   = "pl_read"
	stmtInsert = "pl_insert"
	stmtScan   = "pl_scan"
)

// statements returns the logical-name→SQL map for the hot queries. update reuses
// the insert statement (see planFor), so there is no separate update entry.
func statements() map[string]string {
	return map[string]string{
		stmtRead:   readSQL,
		stmtInsert: insertSQL,
		stmtScan:   scanSQL,
	}
}

// opKind classifies how Execute reads back the result of a statement.
type opKind int

const (
	kindRead  opKind = iota // single-row read into Observed ([]byte)
	kindWrite               // no rows read back; append-only INSERT
	kindScan                // multi-row read into Observed ([][]byte)
)

// opPlan is the resolved execution plan for one operation: the SQL to run, the
// positional bind arguments, and how to interpret the result.
type opPlan struct {
	stmt string
	args []any
	kind opKind
}

// planFor maps a domain.Operation onto SQL and its bind arguments. It is a pure
// function so op→SQL selection and argument wiring are unit-testable without a
// server. scanLimit is resolved from the workload once.
//
// ClickHouse has no in-place UPDATE on the hot path: "insert", "w" and "update"
// are all append-only INSERTs of a new (k, v, seq) row. The ReplacingMergeTree
// engine keeps the row with the greatest seq, so the latest write wins; reads
// enforce this explicitly with ORDER BY seq DESC LIMIT 1 (see readSQL).
func planFor(op domain.Operation, scanLimit int) (opPlan, error) {
	switch op.Type {
	case "read", "r":
		return opPlan{stmt: readSQL, args: []any{op.Key}, kind: kindRead}, nil
	case "insert", "w", "update":
		return opPlan{stmt: insertSQL, args: []any{op.Key, string(op.Value), op.Seq}, kind: kindWrite}, nil
	case "scan":
		if scanLimit <= 0 {
			scanLimit = defaultScanLimit
		}
		return opPlan{stmt: scanSQL, args: []any{op.Key, scanLimit}, kind: kindScan}, nil
	default:
		return opPlan{}, fmt.Errorf("clickhousedriver: unsupported op type %q", op.Type)
	}
}

// createTableSQL builds the idempotent DDL for proofload_kv. It is pure so the
// replicated-vs-standalone engine choice is unit-testable.
//
// Clustered: CREATE ... ON CLUSTER so the DDL fans out to every replica, with a
// ReplicatedReplacingMergeTree whose znode path and replica name come from each
// server's <macros> ({shard}, {replica}); seq is the version column. Replication
// runs through clickhouse-keeper, so a killed replica's rows survive on its peer
// and reconverge on restart. No Distributed table is created: the load engine
// talks straight to a server replica.
//
// Standalone: a plain ReplacingMergeTree(seq); same latest-seq-wins semantics
// without Keeper or replication. This must stay in sync with
// targets/clickhouse/schema/schema.sql.
func createTableSQL(clustered bool) string {
	if clustered {
		return fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ON CLUSTER '%s' %s "+
				"ENGINE = ReplicatedReplacingMergeTree("+
				"'/clickhouse/tables/{shard}/%s', '{replica}', seq) ORDER BY k",
			tableName, clusterName, columnsDDL, tableName)
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s %s ENGINE = ReplacingMergeTree(seq) ORDER BY k",
		tableName, columnsDDL)
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
