package pgdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/proofload/proofload/core/domain"
)

// pgConn is a single PostgreSQL session used by one runner goroutine.
type pgConn struct {
	conn      *pgx.Conn
	scanLimit int
}

// applyConsistency sets the session-wide default transaction isolation level so
// every implicit single-statement transaction runs at the requested level.
func (c *pgConn) applyConsistency(ctx context.Context, consistency string) error {
	lvl, err := isoLevel(consistency)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", lvl)
	if _, err := c.conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("pgdriver: set isolation %q: %w", lvl, err)
	}
	return nil
}

// Prepare registers the hot-path prepared statements and captures the scan
// limit from the workload. It is called once after Connect.
func (c *pgConn) Prepare(ctx context.Context, w domain.Workload) error {
	c.scanLimit = scanLimitFromWorkload(w)
	for name, sql := range statements() {
		if _, err := c.conn.Prepare(ctx, name, sql); err != nil {
			return fmt.Errorf("pgdriver: prepare %q: %w", name, err)
		}
	}
	return nil
}

// Execute runs exactly one operation via its prepared statement. It never times
// itself; errors are returned inside the OpResult so the runner can bucket them.
func (c *pgConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	res := domain.OpResult{Type: op.Type}
	plan, err := planFor(op, c.scanLimit)
	if err != nil {
		res.Err = err
		return res
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
// missing key is not an error: Observed stays nil and Rows is 0.
func (c *pgConn) execRead(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	var v []byte
	err := c.conn.QueryRow(ctx, plan.stmt, plan.args...).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res
		}
		res.Err = err
		return res
	}
	res.Observed = v
	res.Rows = 1
	return res
}

// execWrite runs an upsert or update, reporting affected rows.
func (c *pgConn) execWrite(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	tag, err := c.conn.Exec(ctx, plan.stmt, plan.args...)
	if err != nil {
		res.Err = err
		return res
	}
	res.Rows = int(tag.RowsAffected())
	return res
}

// execScan runs a ranged read, collecting the value bytes into Observed.
func (c *pgConn) execScan(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
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
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			res.Err = err
			return res
		}
		out = append(out, v)
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
func (c *pgConn) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close(context.Background())
}
