package redisdriver

import (
	"fmt"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// defaultScanLimit is used when a workload does not set params.scan_limit.
const defaultScanLimit = 100

// Consistency level identifiers exposed by this target.
const (
	consNone = "none"
	consWait = "wait"
)

// WAIT defaults applied when a workload/target does not tune them.
const (
	defaultWaitReplicas = 1
	defaultWaitTimeout  = 100 * time.Millisecond
)

// keyName renders the canonical proofload Redis key for a numeric key. The
// braces form a Redis Cluster hash tag so co-scanned ranges can share a slot.
func keyName(k int64) string {
	return fmt.Sprintf("proofload:{%d}", k)
}

// cmdKind classifies how Execute issues and reads back a command.
type cmdKind int

const (
	cmdGet  cmdKind = iota // single GET into Observed ([]byte)
	cmdSet                 // SET; no value read back; Rows = 1
	cmdScan                // MGET range into Observed ([][]byte)
)

// opPlan is the resolved execution plan for one operation: which command to run,
// the formatted key(s), and the value for writes.
type opPlan struct {
	kind  cmdKind
	key   string   // for cmdGet / cmdSet
	keys  []string // for cmdScan
	value []byte   // for cmdSet
}

// planFor maps a domain.Operation onto a Redis command and its arguments. It is
// a pure function so the op→command selection and key wiring are unit testable
// without a server. scanLimit is resolved from the workload once.
func planFor(op domain.Operation, scanLimit int) (opPlan, error) {
	switch op.Type {
	case "read", "r":
		return opPlan{kind: cmdGet, key: keyName(op.Key)}, nil
	case "insert", "w", "update":
		return opPlan{kind: cmdSet, key: keyName(op.Key), value: op.Value}, nil
	case "scan":
		if scanLimit <= 0 {
			scanLimit = defaultScanLimit
		}
		return opPlan{kind: cmdScan, keys: scanKeys(op.Key, scanLimit)}, nil
	default:
		return opPlan{}, fmt.Errorf("redisdriver: unsupported op type %q", op.Type)
	}
}

// scanKeys builds the contiguous key range [start, start+n) for an MGET scan.
func scanKeys(start int64, n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = keyName(start + int64(i))
	}
	return keys
}

// consistency is the resolved per-run durability policy for writes.
type consistency struct {
	level    string
	replicas int
	timeout  time.Duration
}

// parseConsistency maps a driver.Config.Consistency string to a resolved policy.
// The empty string and "none" mean fire-and-forget (async replication). "wait"
// and "waitN" issue a WAIT after each write, with replica count and timeout
// taken from params. Unknown levels are rejected so misconfiguration fails fast.
func parseConsistency(level string, params map[string]any) (consistency, error) {
	switch level {
	case "", consNone:
		return consistency{level: consNone}, nil
	case consWait, "waitN":
		return consistency{
			level:    consWait,
			replicas: waitReplicas(params),
			timeout:  waitTimeout(params),
		}, nil
	default:
		return consistency{}, fmt.Errorf("redisdriver: unsupported consistency %q", level)
	}
}

// waitReplicas reads the WAIT replica count from params (wait_replicas, then
// numreplicas), falling back to defaultWaitReplicas.
func waitReplicas(params map[string]any) int {
	for _, key := range []string{"wait_replicas", "numreplicas"} {
		if n := paramInt(params, key); n > 0 {
			return n
		}
	}
	return defaultWaitReplicas
}

// waitTimeout reads the WAIT timeout in milliseconds from params
// (wait_timeout_ms), falling back to defaultWaitTimeout.
func waitTimeout(params map[string]any) time.Duration {
	if n := paramInt(params, "wait_timeout_ms"); n > 0 {
		return time.Duration(n) * time.Millisecond
	}
	return defaultWaitTimeout
}

// scanLimitFromWorkload reads params.scan_limit (or params.limit) from a
// workload, falling back to defaultScanLimit. Values arrive from YAML as int or
// float64 depending on the parser, so both are accepted.
func scanLimitFromWorkload(w domain.Workload) int {
	for _, key := range []string{"scan_limit", "limit"} {
		if n := paramInt(w.Params, key); n > 0 {
			return n
		}
	}
	return defaultScanLimit
}

// paramInt reads an int-valued param, tolerating a nil map and the int/int64/
// float64 shapes YAML decoders produce. It returns 0 when absent or non-numeric.
func paramInt(params map[string]any, key string) int {
	if params == nil {
		return 0
	}
	switch n := params[key].(type) {
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
