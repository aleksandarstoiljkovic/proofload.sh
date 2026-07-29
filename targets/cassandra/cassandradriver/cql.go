package cassandradriver

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/domain"
)

// keyspaceName and tableName name the fixed proofload schema. Connect sets the
// session keyspace to keyspaceName, so the hot-path statements reference the
// table unqualified.
const (
	keyspaceName = "proofload"
	tableName    = "kv"
)

// Logical statement names, used as the keys of statements() so tests can assert
// every hot operation has CQL without depending on the CQL text itself.
const (
	stmtRead   = "pl_read"
	stmtInsert = "pl_insert"
	stmtUpdate = "pl_update"
	stmtScan   = "pl_scan"
)

// defaultScanLimit is used when a workload does not set params.scan_limit.
const defaultScanLimit = 100

// Hot-path CQL. gocql prepares and caches these by statement text on first use,
// so passing them to Session.Query yields prepared/bound execution.
//
// scanCQL uses token(k) >= token(?) rather than k >= ?: k is the partition key,
// whose on-disk order is by token (hash), not by value, so a value range like
// "k >= ?" is not a valid restriction without ALLOW FILTERING (a full cluster
// scan). Scanning by token range is the idiomatic Cassandra bounded scan; see
// planFor's caveat.
const (
	readCQL   = `SELECT v FROM kv WHERE k = ?`
	insertCQL = `INSERT INTO kv (k, v, seq) VALUES (?, ?, ?)`
	updateCQL = `UPDATE kv SET v = ?, seq = ? WHERE k = ?`
	scanCQL   = `SELECT k, v FROM kv WHERE token(k) >= token(?) LIMIT ?`
)

// statements returns the logical-name→CQL map for the hot queries. It is the
// single source of truth for testing that every operation kind is covered.
func statements() map[string]string {
	return map[string]string{
		stmtRead:   readCQL,
		stmtInsert: insertCQL,
		stmtUpdate: updateCQL,
		stmtScan:   scanCQL,
	}
}

// opKind classifies how Execute reads back the result of a statement.
type opKind int

const (
	kindRead  opKind = iota // single-row read into Observed ([]byte)
	kindWrite               // no rows read back; upsert/update
	kindScan                // multi-row read into Observed ([][]byte)
)

// opPlan is the resolved execution plan for one operation: the CQL to run, the
// positional bind arguments, and how to interpret the result.
type opPlan struct {
	stmt string
	args []any
	kind opKind
}

// planFor maps a domain.Operation onto CQL and its bind arguments. It is a pure
// function so op→CQL selection and argument wiring are unit-testable without a
// cluster. scanLimit is resolved from the workload once.
//
// Cassandra has no distinct upsert: "insert" and "update" are both blind writes
// (last-write-wins), so INSERT provides the upsert semantics the runner expects.
func planFor(op domain.Operation, scanLimit int) (opPlan, error) {
	switch op.Type {
	case "read", "r":
		return opPlan{stmt: readCQL, args: []any{op.Key}, kind: kindRead}, nil
	case "insert", "w":
		return opPlan{stmt: insertCQL, args: []any{op.Key, op.Value, op.Seq}, kind: kindWrite}, nil
	case "update":
		return opPlan{stmt: updateCQL, args: []any{op.Value, op.Seq, op.Key}, kind: kindWrite}, nil
	case "scan":
		if scanLimit <= 0 {
			scanLimit = defaultScanLimit
		}
		return opPlan{stmt: scanCQL, args: []any{op.Key, scanLimit}, kind: kindScan}, nil
	default:
		return opPlan{}, fmt.Errorf("cassandradriver: unsupported op type %q", op.Type)
	}
}

// supportedConsistency lists the levels this target exposes, ordered weakest to
// strongest. Kept as a package var so consistencyLevel, ConsistencyLevels, and
// tests share one definition.
var supportedConsistency = []string{"one", "quorum", "local_quorum", "all"}

// consistencyLevel maps a driver.Config.Consistency string to a gocql
// consistency. The empty string defaults to QUORUM. Unknown levels are rejected
// so misconfiguration fails fast.
func consistencyLevel(consistency string) (gocql.Consistency, error) {
	switch consistency {
	case "", "quorum":
		return gocql.Quorum, nil
	case "one":
		return gocql.One, nil
	case "local_quorum", "local-quorum":
		return gocql.LocalQuorum, nil
	case "all":
		return gocql.All, nil
	default:
		return 0, fmt.Errorf("cassandradriver: unsupported consistency %q", consistency)
	}
}

// createKeyspaceCQL builds the idempotent keyspace DDL. It is pure so the
// SimpleStrategy/replication_factor wiring is unit-testable. rf is clamped to a
// minimum of 1 by resolveReplicationFactor before it reaches here.
func createKeyspaceCQL(keyspace string, rf int) string {
	return fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "+
			"{'class': 'SimpleStrategy', 'replication_factor': %d}",
		keyspace, rf)
}

// createTableCQL builds the idempotent table DDL, keyspace-qualified because
// Schema runs it before the session keyspace is set.
func createTableCQL(keyspace string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s "+
			"(k bigint PRIMARY KEY, v blob, seq bigint)",
		keyspace, tableName)
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
