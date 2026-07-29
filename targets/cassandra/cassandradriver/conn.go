package cassandradriver

import (
	"context"
	"errors"

	"github.com/gocql/gocql"
	"github.com/proofload/proofload/core/domain"
)

// cassConn is a single Cassandra session used by one runner goroutine.
type cassConn struct {
	session   *gocql.Session
	scanLimit int
}

// Prepare captures the scan limit from the workload. gocql prepares and caches
// statements by text on first Execute, so there is no explicit prepare step.
func (c *cassConn) Prepare(_ context.Context, w domain.Workload) error {
	c.scanLimit = scanLimitFromWorkload(w)
	return nil
}

// Execute runs exactly one operation via a bound (prepared) query. It never
// times itself; errors are returned inside the OpResult so the runner can
// bucket them per operation type.
func (c *cassConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	plan, err := planFor(op, c.scanLimit)
	if err != nil {
		return domain.OpResult{Type: op.Type, Err: err}
	}
	switch plan.kind {
	case kindRead:
		return c.execRead(ctx, op.Type, plan)
	case kindScan:
		return c.execScan(ctx, op.Type, plan)
	default:
		return c.execWrite(ctx, op.Type, plan)
	}
}

// execRead runs a single-row read, placing the value bytes in Observed. A
// missing key is not an error: Observed stays nil and Rows is 0 (gocql reports
// ErrNotFound when the SELECT matches no row).
func (c *cassConn) execRead(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	var v []byte
	err := c.session.Query(plan.stmt, plan.args...).WithContext(ctx).Scan(&v)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return res
		}
		res.Err = err
		return res
	}
	res.Observed = v
	res.Rows = 1
	return res
}

// execWrite runs an INSERT (upsert) or UPDATE. CQL blind writes report no
// affected-row count, so Rows is set to 1 on success.
func (c *cassConn) execWrite(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	if err := c.session.Query(plan.stmt, plan.args...).WithContext(ctx).Exec(); err != nil {
		res.Err = err
		return res
	}
	res.Rows = 1
	return res
}

// execScan runs a token-range read, collecting the value bytes into Observed.
// Rows returned are in token (hash) order, not key order — see scanCQL.
func (c *cassConn) execScan(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	iter := c.session.Query(plan.stmt, plan.args...).WithContext(ctx).Iter()

	var out [][]byte
	var k int64
	var v []byte
	for iter.Scan(&k, &v) {
		val := make([]byte, len(v))
		copy(val, v)
		out = append(out, val)
	}
	if err := iter.Close(); err != nil {
		res.Err = err
		return res
	}
	res.Observed = out
	res.Rows = len(out)
	return res
}

// Close releases the session.
func (c *cassConn) Close() error {
	if c.session != nil {
		c.session.Close()
	}
	return nil
}
