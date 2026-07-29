package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/testutil"
)

// --- test fakes -------------------------------------------------------------

// seqGen is a per-connection generator producing an alternating read/write mix.
type seqGen struct{ n int64 }

func (g *seqGen) Next() domain.Operation {
	k := atomic.AddInt64(&g.n, 1)
	op := domain.Operation{Type: "read", Key: k % 128, Seq: k}
	if k%2 == 0 {
		op.Type = "write"
		op.Value = []byte("v")
	}
	return op
}

func genFn(int) OpGen { return &seqGen{} }

// nowSched is a closed-loop scheduler: every slot is "now", so operations fire
// as fast as the connections can serve them.
type nowSched struct{}

func (nowSched) Next() time.Time { return time.Now() }

// openSched is an open-loop scheduler handing out fixed-rate arrival times from
// a shared counter, independent of how fast the client actually keeps up.
type openSched struct {
	start    time.Time
	interval time.Duration
	n        int64
}

func (s *openSched) Next() time.Time {
	k := atomic.AddInt64(&s.n, 1) - 1
	return s.start.Add(time.Duration(k) * s.interval)
}

// countingSink counts records (and errors) for one shard.
type countingSink struct {
	count int64
	errs  int64
}

func (s *countingSink) Record(_ domain.OpType, _ time.Duration, isErr bool) {
	atomic.AddInt64(&s.count, 1)
	if isErr {
		atomic.AddInt64(&s.errs, 1)
	}
}

// sinksFor builds n per-shard counting sinks plus the builder that hands them out.
func sinksFor(n int) (func(int) Sink, []*countingSink) {
	sinks := make([]*countingSink, n)
	for i := range sinks {
		sinks[i] = &countingSink{}
	}
	return func(shard int) Sink { return sinks[shard] }, sinks
}

func sinkSum(sinks []*countingSink) int64 {
	var sum int64
	for _, s := range sinks {
		sum += atomic.LoadInt64(&s.count)
	}
	return sum
}

// trackDriver counts opened and closed connections, so tests can assert every
// connection is closed on shutdown.
type trackDriver struct {
	latency time.Duration
	opened  int64
	closed  int64
}

func (d *trackDriver) Name() string { return "track" }

func (d *trackDriver) Schema(context.Context, driver.Config, domain.Workload) error { return nil }

func (d *trackDriver) Connect(context.Context, driver.Config) (driver.Conn, error) {
	atomic.AddInt64(&d.opened, 1)
	return &trackConn{d: d}, nil
}

type trackConn struct{ d *trackDriver }

func (c *trackConn) Prepare(context.Context, domain.Workload) error { return nil }

func (c *trackConn) Execute(ctx context.Context, op domain.Operation) domain.OpResult {
	if c.d.latency > 0 {
		select {
		case <-time.After(c.d.latency):
		case <-ctx.Done():
			return domain.OpResult{Type: op.Type, Err: ctx.Err()}
		}
	}
	return domain.OpResult{Type: op.Type, Rows: 1}
}

func (c *trackConn) Close() error {
	atomic.AddInt64(&c.d.closed, 1)
	return nil
}

// --- tests ------------------------------------------------------------------

// (1) Closed loop with tiny latency and a short duration drives real work.
func TestRun_ClosedLoop(t *testing.T) {
	d := testutil.NewFakeDriver()
	d.Latency = 50 * time.Microsecond
	sinkFn, sinks := sinksFor(4)

	rep, err := Run(context.Background(), Deps{
		Driver: d,
		Sched:  nowSched{},
		Gen:    genFn,
		Sink:   sinkFn,
	}, Options{Connections: 4, Duration: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Total == 0 {
		t.Fatal("expected Total > 0")
	}
	if got := sinkSum(sinks); got != rep.Total {
		t.Fatalf("sink sum %d != report total %d", got, rep.Total)
	}
	if rep.Errors != 0 {
		t.Fatalf("expected no errors, got %d", rep.Errors)
	}
}

// (2) FailEvery=3 yields errors at roughly one third of measured operations.
func TestRun_ErrorsFromResult(t *testing.T) {
	d := testutil.NewFakeDriver()
	d.FailEvery = 3
	sinkFn, sinks := sinksFor(1)

	rep, err := Run(context.Background(), Deps{
		Driver: d,
		Sched:  nowSched{},
		Gen:    genFn,
		Sink:   sinkFn,
	}, Options{Connections: 1, Duration: 60 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Total < 3 {
		t.Fatalf("too few operations to test error rate: %d", rep.Total)
	}
	want := rep.Total / 3
	if diff := rep.Errors - want; diff < -1 || diff > 1 {
		t.Fatalf("errors %d not ~= total/3 (%d)", rep.Errors, want)
	}
	if rep.Errors != atomic.LoadInt64(&sinks[0].errs) {
		t.Fatalf("report errors %d != sink errors %d", rep.Errors, sinks[0].errs)
	}
}

// (3) Warmup operations are excluded from the measure sink and the report.
func TestRun_WarmupExcluded(t *testing.T) {
	d := testutil.NewFakeDriver()
	d.Latency = 50 * time.Microsecond
	sinkFn, sinks := sinksFor(2)

	rep, err := Run(context.Background(), Deps{
		Driver: d,
		Sched:  nowSched{},
		Gen:    genFn,
		Sink:   sinkFn,
	}, Options{Connections: 2, Warmup: 60 * time.Millisecond, Duration: 60 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := sinkSum(sinks); got != rep.Total {
		t.Fatalf("sink sum %d != report total %d", got, rep.Total)
	}
	// The fake executed both warmup and measure operations; the report and the
	// sinks only reflect measure operations, so the fake must have seen more.
	fakeOps := int64(len(d.Operations()))
	if fakeOps <= rep.Total {
		t.Fatalf("expected warmup ops beyond measured: fake=%d measured=%d", fakeOps, rep.Total)
	}
}

// (4) Every connection is actually used (each conn executes >= 1 measured op).
func TestRun_AllConnectionsUsed(t *testing.T) {
	d := testutil.NewFakeDriver()
	d.Latency = 50 * time.Microsecond
	const conns = 6
	sinkFn, sinks := sinksFor(conns)

	rep, err := Run(context.Background(), Deps{
		Driver: d,
		Sched:  nowSched{},
		Gen:    genFn,
		Sink:   sinkFn,
	}, Options{Connections: conns, Duration: 150 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Total < conns {
		t.Fatalf("expected at least %d operations, got %d", conns, rep.Total)
	}
	for i, s := range sinks {
		if atomic.LoadInt64(&s.count) == 0 {
			t.Errorf("connection %d executed no measured operations", i)
		}
	}
}

// (5) Cancelling ctx mid-run returns promptly and closes every connection.
func TestRun_ContextCancel(t *testing.T) {
	d := &trackDriver{latency: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := Run(ctx, Deps{
		Driver: d,
		Sched:  nowSched{},
		Gen:    genFn,
		Sink:   nil, // nil-safe: exercises the discard-sink path
	}, Options{Connections: 4, Duration: 10 * time.Second})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("did not stop promptly after cancel: %v", elapsed)
	}
	if opened := atomic.LoadInt64(&d.opened); opened != 4 {
		t.Fatalf("expected 4 connections opened, got %d", opened)
	}
	if closed := atomic.LoadInt64(&d.closed); closed != atomic.LoadInt64(&d.opened) {
		t.Fatalf("expected all connections closed, opened=%d closed=%d", d.opened, closed)
	}
}

// (6) ClientBound trips when per-op latency dwarfs the open-loop arrival rate.
func TestRun_ClientBound(t *testing.T) {
	d := testutil.NewFakeDriver()
	d.Latency = 20 * time.Millisecond
	sched := &openSched{start: time.Now(), interval: time.Millisecond}

	rep, err := Run(context.Background(), Deps{
		Driver: d,
		Sched:  sched,
		Gen:    genFn,
		Sink:   nil,
	}, Options{Connections: 1, Duration: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !rep.ClientBound {
		t.Fatalf("expected ClientBound, meanLateness=%v total=%d", rep.MeanLateness, rep.Total)
	}
	if rep.MeanLateness <= 0 {
		t.Fatalf("expected positive mean lateness, got %v", rep.MeanLateness)
	}
}

// Guard: invalid options fail fast before any connection is opened.
func TestRun_Validate(t *testing.T) {
	base := Deps{Driver: testutil.NewFakeDriver(), Sched: nowSched{}, Gen: genFn}
	cases := []struct {
		name string
		o    Options
	}{
		{"zero connections", Options{Connections: 0, Duration: time.Second}},
		{"zero duration", Options{Connections: 1, Duration: 0}},
		{"negative warmup", Options{Connections: 1, Duration: time.Second, Warmup: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Run(context.Background(), base, tc.o); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}
