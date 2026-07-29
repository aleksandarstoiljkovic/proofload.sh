package clickhousedriver

import (
	"context"
	"database/sql"
	"errors"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/proofload/proofload/core/domain"
)

// chConn is a single ClickHouse session used by one runner goroutine.
type chConn struct {
	conn      chdriver.Conn
	scanLimit int
}

// Prepare captures the scan limit from the workload. clickhouse-go binds query
// parameters client-side per call, so there is no separate prepare step.
func (c *chConn) Prepare(_ context.Context, w domain.Workload) error {
	c.scanLimit = scanLimitFromWorkload(w)
	return nil
}

// Execute runs exactly one operation. It never times itself; errors are returned
// inside the OpResult so the runner can bucket them per operation type.
func (c *chConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
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

// execRead runs a single-row read, placing the value bytes in Observed. A missing
// key is not an error: Observed stays nil and Rows is 0 (QueryRow reports
// sql.ErrNoRows when the SELECT matches no row).
func (c *chConn) execRead(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	var v string
	err := c.conn.QueryRow(ctx, plan.stmt, plan.args...).Scan(&v)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return res
		}
		res.Err = err
		return res
	}
	res.Observed = []byte(v)
	res.Rows = 1
	return res
}

// execWrite runs an append-only INSERT. ClickHouse INSERTs report no affected-row
// count, so Rows is set to 1 on success.
func (c *chConn) execWrite(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	if err := c.conn.Exec(ctx, plan.stmt, plan.args...); err != nil {
		res.Err = err
		return res
	}
	res.Rows = 1
	return res
}

// execScan runs a ranged read, collecting the value bytes into Observed in key
// order.
func (c *chConn) execScan(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	rows, err := c.conn.Query(ctx, plan.stmt, plan.args...)
	if err != nil {
		res.Err = err
		return res
	}
	defer rows.Close()

	var out [][]byte
	for rows.Next() {
		var k int64
		var v string
		if err := rows.Scan(&k, &v); err != nil {
			res.Err = err
			return res
		}
		out = append(out, []byte(v))
	}
	if err := rows.Err(); err != nil {
		res.Err = err
		return res
	}
	res.Observed = out
	res.Rows = len(out)
	return res
}

// Close releases the connection.
func (c *chConn) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
