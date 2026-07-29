package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// defaultStartLead is the fixed gap added after the join barrier is reached
	// before T0, so every worker begins its measure phase together.
	defaultStartLead = 500 * time.Millisecond
	// defaultMaxWait bounds Serve when neither ctx nor spec durations do.
	defaultMaxWait = 5 * time.Minute
	// shutdownGrace bounds the graceful HTTP server shutdown.
	shutdownGrace = 2 * time.Second
)

// ErrIncomplete is returned by Serve when its deadline elapses before every
// worker has submitted.
var ErrIncomplete = errors.New("cluster: deadline reached before all workers submitted")

// Coordinator runs the HTTP control plane for one distributed run: a join
// barrier with a synchronized start-gun, plus result collection.
type Coordinator struct {
	spec        RunSpec
	workerCount int

	// StartLead is the lead added to the barrier time to compute T0. Optional;
	// set before Serve. Defaults to defaultStartLead when non-positive.
	StartLead time.Duration
	// MaxWait bounds how long Serve blocks awaiting submissions. Optional; set
	// before Serve. When zero a value is derived from the spec durations.
	MaxWait time.Duration

	wantAddr string

	mu      sync.Mutex
	joined  map[int]bool
	startAt time.Time
	reports map[int]WorkerReport
	histos  map[int][]byte

	joinedAll    chan struct{} // closed once all workers have joined (T0 set)
	submittedAll chan struct{} // closed once all workers have submitted
	stop         chan struct{} // closed as Serve returns, to release long-polls
	joinOnce     sync.Once
	submitOnce   sync.Once

	ready     chan struct{} // closed once the listener is bound (or bind failed)
	readyOnce sync.Once
	boundAddr string
}

// NewCoordinator creates a coordinator that will listen on addr (use
// "127.0.0.1:0" for an ephemeral port; read the real address back via Addr).
func NewCoordinator(addr string, spec RunSpec, workerCount int) *Coordinator {
	return &Coordinator{
		spec:         spec,
		workerCount:  workerCount,
		StartLead:    defaultStartLead,
		wantAddr:     addr,
		joined:       make(map[int]bool),
		reports:      make(map[int]WorkerReport),
		histos:       make(map[int][]byte),
		joinedAll:    make(chan struct{}),
		submittedAll: make(chan struct{}),
		stop:         make(chan struct{}),
		ready:        make(chan struct{}),
	}
}

// Addr blocks until Serve has bound its listener, then returns the resolved
// listen address (empty if binding failed).
func (c *Coordinator) Addr() string {
	<-c.ready
	return c.boundAddr
}

// Serve binds the listener, runs the HTTP control plane, and blocks until all
// workers have submitted (returning the aggregated Result), ctx is canceled, or
// the deadline elapses. It always shuts the server down before returning.
func (c *Coordinator) Serve(ctx context.Context) (Result, error) {
	ln, err := net.Listen("tcp", c.wantAddr)
	if err != nil {
		c.readyOnce.Do(func() { close(c.ready) })
		return Result{}, err
	}
	c.boundAddr = ln.Addr().String()
	c.readyOnce.Do(func() { close(c.ready) })

	mux := http.NewServeMux()
	mux.HandleFunc("/join", c.handleJoin)
	mux.HandleFunc("/submit", c.handleSubmit)
	srv := &http.Server{Handler: mux}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	timer := time.NewTimer(c.maxWait())
	defer timer.Stop()

	var retErr error
	select {
	case <-c.submittedAll:
	case <-ctx.Done():
		retErr = ctx.Err()
	case <-timer.C:
		retErr = ErrIncomplete
	case retErr = <-serveErr:
	}

	close(c.stop) // release any long-polling /join handlers
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	if retErr != nil {
		return Result{}, retErr
	}
	return c.result(), nil
}

func (c *Coordinator) startLead() time.Duration {
	if c.StartLead <= 0 {
		return defaultStartLead
	}
	return c.StartLead
}

func (c *Coordinator) maxWait() time.Duration {
	if c.MaxWait > 0 {
		return c.MaxWait
	}
	if d := c.spec.Warmup + c.spec.Duration + c.startLead(); d > 0 {
		return d + defaultMaxWait
	}
	return defaultMaxWait
}
