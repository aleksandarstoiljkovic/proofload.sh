package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// joinOutcome carries one worker's Join result across goroutines.
type joinOutcome struct {
	id    int
	spec  RunSpec
	start time.Time
	err   error
}

// TestCoordinatorRoundTrip runs a full 3-worker cycle in-process: all workers
// Join (distinct ids, identical spec + T0), Submit synthetic reports+blobs, and
// Serve returns an aggregated Result with one entry per worker.
func TestCoordinatorRoundTrip(t *testing.T) {
	spec := RunSpec{Target: "postgres", Workload: "ycsb-a", WorkerCount: 3, Duration: time.Second}
	c := NewCoordinator("127.0.0.1:0", spec, 3)
	c.StartLead = 300 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		r, err := c.Serve(ctx)
		resCh <- r
		errCh <- err
	}()

	addr := c.Addr()
	joins := make(chan joinOutcome, 3)
	for i := 0; i < 3; i++ {
		go func() {
			w := NewWorker(addr, -1) // request auto-assignment
			s, st, err := w.Join(ctx)
			joins <- joinOutcome{id: w.ID(), spec: s, start: st, err: err}
			if err != nil {
				return
			}
			rep := WorkerReport{WorkerID: w.ID(), Total: int64(100 + w.ID()), Errors: 1}
			blob := []byte(fmt.Sprintf("histogram-blob-%d", w.ID()))
			if err := w.Submit(ctx, rep, blob); err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
	}

	ids := make(map[int]bool)
	starts := make([]time.Time, 0, 3)
	for i := 0; i < 3; i++ {
		jo := <-joins
		if jo.err != nil {
			t.Fatalf("join: %v", jo.err)
		}
		if jo.spec.Target != "postgres" || jo.spec.WorkerCount != 3 {
			t.Errorf("worker %d got wrong spec: %+v", jo.id, jo.spec)
		}
		ids[jo.id] = true
		starts = append(starts, jo.start)
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 distinct worker ids, got %v", ids)
	}
	for _, want := range []int{0, 1, 2} {
		if !ids[want] {
			t.Errorf("missing expected id %d in %v", want, ids)
		}
	}
	for _, s := range starts[1:] {
		if !s.Equal(starts[0]) {
			t.Errorf("start times differ: %v vs %v", starts[0], s)
		}
	}

	res := <-resCh
	if err := <-errCh; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if len(res.Reports) != 3 {
		t.Fatalf("want 3 reports, got %d", len(res.Reports))
	}
	if len(res.Histograms) != 3 {
		t.Fatalf("want 3 histogram blobs, got %d", len(res.Histograms))
	}
	for i, rep := range res.Reports {
		if rep.WorkerID != i {
			t.Errorf("reports not ordered by id: index %d has id %d", i, rep.WorkerID)
		}
		if len(res.Histograms[i]) == 0 {
			t.Errorf("empty histogram blob for worker %d", i)
		}
	}
}

// TestStartGunSynchronized asserts every worker receives the SAME T0 and that
// T0 is in the near future at join time.
func TestStartGunSynchronized(t *testing.T) {
	c := NewCoordinator("127.0.0.1:0", RunSpec{WorkerCount: 2}, 2)
	c.StartLead = 2 * time.Second // large lead so the assertion is not flaky

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _, _ = c.Serve(ctx) }()

	addr := c.Addr()
	got := make(chan time.Time, 2)
	for i := 0; i < 2; i++ {
		go func() {
			w := NewWorker(addr, -1)
			_, st, err := w.Join(ctx)
			if err != nil {
				t.Errorf("join: %v", err)
				got <- time.Time{}
				return
			}
			got <- st
			_ = w.Submit(ctx, WorkerReport{WorkerID: w.ID()}, nil)
		}()
	}

	a, b := <-got, <-got
	if !a.Equal(b) {
		t.Errorf("start times not synchronized: %v vs %v", a, b)
	}
	if !a.After(time.Now()) {
		t.Errorf("start gun %v is not in the future", a)
	}
}

// TestServeCtxCancel confirms Serve returns promptly with the ctx error when its
// context is canceled before all workers submit.
func TestServeCtxCancel(t *testing.T) {
	c := NewCoordinator("127.0.0.1:0", RunSpec{WorkerCount: 3}, 3)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := c.Serve(ctx)
		done <- err
	}()
	_ = c.Addr() // ensure the server is bound before canceling

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return promptly after ctx cancel")
	}
}

// TestSubmitUnknownID confirms a submission from a never-joined worker id is
// rejected.
func TestSubmitUnknownID(t *testing.T) {
	c := NewCoordinator("127.0.0.1:0", RunSpec{WorkerCount: 2}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = c.Serve(ctx) }()

	addr := c.Addr()
	w := NewWorker(addr, 0)
	if err := w.Submit(ctx, WorkerReport{WorkerID: 99}, []byte("x")); err == nil {
		t.Fatal("expected rejection of unknown worker id, got nil error")
	}
}

// TestRegisterAssignment table-drives the WorkerID assignment logic: auto,
// preassigned, collision reassignment, out-of-range fallback, and overflow.
func TestRegisterAssignment(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		requests []int
		wantIDs  []int
		wantOK   []bool
	}{
		{"auto assign sequential", 3, []int{-1, -1, -1}, []int{0, 1, 2}, []bool{true, true, true}},
		{"honor preassigned", 3, []int{2, 0, 1}, []int{2, 0, 1}, []bool{true, true, true}},
		{"collision reassigns", 3, []int{0, 0, 0}, []int{0, 1, 2}, []bool{true, true, true}},
		{"out of range falls back", 2, []int{9, -1}, []int{0, 1}, []bool{true, true}},
		{"overflow rejected", 2, []int{-1, -1, -1}, []int{0, 1, -1}, []bool{true, true, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCoordinator("127.0.0.1:0", RunSpec{WorkerCount: tt.count}, tt.count)
			for i, req := range tt.requests {
				id, ok := c.register(req)
				if id != tt.wantIDs[i] || ok != tt.wantOK[i] {
					t.Errorf("register(%d) call %d = (%d, %v); want (%d, %v)",
						req, i, id, ok, tt.wantIDs[i], tt.wantOK[i])
				}
			}
		})
	}
}
