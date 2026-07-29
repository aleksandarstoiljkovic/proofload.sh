// Package testutil provides in-memory fakes so core packages can be tested
// without a real database. FakeDriver implements driver.Driver/Conn and records
// every executed operation, letting the runner and verifiers be exercised
// deterministically.
package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// FakeDriver is a configurable, in-memory driver.Driver for tests.
type FakeDriver struct {
	// Latency, if set, is slept inside Execute to simulate work.
	Latency time.Duration
	// FailEvery causes every Nth Execute to return an error (0 = never).
	FailEvery int

	mu    sync.Mutex
	store map[int64][]byte
	ops   []domain.Operation
	n     int
}

// NewFakeDriver returns a ready FakeDriver.
func NewFakeDriver() *FakeDriver {
	return &FakeDriver{store: make(map[int64][]byte)}
}

// Name implements driver.Driver.
func (d *FakeDriver) Name() string { return "fake" }

// Schema implements driver.Driver (no-op for the fake).
func (d *FakeDriver) Schema(context.Context, driver.Config, domain.Workload) error { return nil }

// Connect implements driver.Driver, returning a Conn backed by shared state.
func (d *FakeDriver) Connect(context.Context, driver.Config) (driver.Conn, error) {
	return &fakeConn{d: d}, nil
}

// Operations returns a copy of every operation executed so far.
func (d *FakeDriver) Operations() []domain.Operation {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]domain.Operation, len(d.ops))
	copy(out, d.ops)
	return out
}

type fakeConn struct{ d *FakeDriver }

func (c *fakeConn) Prepare(context.Context, domain.Workload) error { return nil }

func (c *fakeConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	d := c.d
	if d.Latency > 0 {
		select {
		case <-time.After(d.Latency):
		case <-ctx.Done():
			return domain.OpResult{Type: op.Type, Err: ctx.Err()}
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.n++
	d.ops = append(d.ops, op)
	if d.FailEvery > 0 && d.n%d.FailEvery == 0 {
		return domain.OpResult{Type: op.Type, Err: errFakeExecute}
	}
	res := domain.OpResult{Type: op.Type, Rows: 1}
	if len(op.Value) > 0 {
		d.store[op.Key] = op.Value
	} else {
		res.Observed = d.store[op.Key]
	}
	return res
}

func (c *fakeConn) Close() error { return nil }

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

const errFakeExecute = fakeErr("fake: injected execute failure")
