package redisdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/proofload/proofload/core/domain"
	"github.com/redis/go-redis/v9"
)

// redisConn is a single Redis session used by one runner goroutine.
type redisConn struct {
	client    *redis.Client
	scanLimit int
	cons      consistency
}

// Prepare captures the scan limit from the workload and validates connectivity
// with a PING. It is called once after Connect, before the first Execute.
func (c *redisConn) Prepare(ctx context.Context, w domain.Workload) error {
	c.scanLimit = scanLimitFromWorkload(w)
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redisdriver: ping: %w", err)
	}
	return nil
}

// Execute runs exactly one operation. It never times itself; errors are returned
// inside the OpResult so the runner can bucket them per operation type.
func (c *redisConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	res := domain.OpResult{Type: op.Type}
	plan, err := planFor(op, c.scanLimit)
	if err != nil {
		res.Err = err
		return res
	}

	switch plan.kind {
	case cmdGet:
		return c.execGet(ctx, op.Type, plan)
	case cmdScan:
		return c.execScan(ctx, op.Type, plan)
	default:
		return c.execSet(ctx, op.Type, plan)
	}
}

// execGet runs a single GET, placing the value bytes in Observed. A missing key
// is not an error: Observed stays nil and Rows is 0.
func (c *redisConn) execGet(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	v, err := c.client.Get(ctx, plan.key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res
		}
		res.Err = err
		return res
	}
	res.Observed = v
	res.Rows = 1
	return res
}

// execSet runs a SET, then issues WAIT when the run's consistency level requests
// synchronous replica acknowledgement. WAIT's own timeout bounds the round-trip,
// so a lagging replica surfaces as a durability shortfall, not a hang.
func (c *redisConn) execSet(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	if err := c.client.Set(ctx, plan.key, plan.value, 0).Err(); err != nil {
		res.Err = err
		return res
	}
	res.Rows = 1
	if c.cons.level == consWait {
		if err := c.client.Wait(ctx, c.cons.replicas, c.cons.timeout).Err(); err != nil {
			res.Err = err
		}
	}
	return res
}

// execScan runs an MGET over a contiguous key range, collecting the present
// value bytes into Observed. Missing members are skipped, so Rows reflects the
// number of keys that actually existed.
func (c *redisConn) execScan(ctx context.Context, t domain.OpType, plan opPlan) domain.OpResult {
	res := domain.OpResult{Type: t}
	vals, err := c.client.MGet(ctx, plan.keys...).Result()
	if err != nil {
		res.Err = err
		return res
	}

	out := make([][]byte, 0, len(vals))
	for _, v := range vals {
		switch val := v.(type) {
		case string:
			out = append(out, []byte(val))
		case []byte:
			out = append(out, val)
		}
	}
	res.Observed = out
	res.Rows = len(out)
	return res
}

// Close releases the connection.
func (c *redisConn) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}
