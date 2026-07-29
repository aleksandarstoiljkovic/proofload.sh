package nemesis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// fakeController records inject/heal calls and tracks the per-node faulted
// balance so tests can assert nothing is left un-healed. It is concurrency-safe
// so it is sound under -race even though Run drives it sequentially.
type fakeController struct {
	mu       sync.Mutex
	injected int
	healed   int
	balance  map[string]int // node ID -> outstanding injections
	blockFor time.Duration  // optional per-inject delay to simulate slow faults
}

func newFake() *fakeController { return &fakeController{balance: map[string]int{}} }

func (f *fakeController) Inject(_ context.Context, flt domain.Fault, node domain.Node) error {
	f.mu.Lock()
	f.injected++
	f.balance[node.ID]++
	f.mu.Unlock()
	if f.blockFor > 0 {
		time.Sleep(f.blockFor)
	}
	return nil
}

func (f *fakeController) Heal(_ context.Context, node domain.Node) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healed++
	if f.balance[node.ID] > 0 {
		f.balance[node.ID]--
	}
	return nil
}

func (f *fakeController) outstanding() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.balance {
		n += v
	}
	return n
}

func threeNodes() []domain.Node {
	return []domain.Node{
		{ID: "n0", Control: domain.ControlEndpoint{Method: domain.ControlDocker, Ref: "n0"}},
		{ID: "n1", Control: domain.ControlEndpoint{Method: domain.ControlDocker, Ref: "n1"}},
		{ID: "n2", Control: domain.ControlEndpoint{Method: domain.ControlDocker, Ref: "n2"}},
	}
}

// TestRunTimelineAndHealing runs a kill (at 50ms, dur 30ms) and a repeating
// partition (at 80ms, repeat 40ms) over a 200ms window and asserts the timeline
// is time-ordered, has an inject+heal for every fault, and leaves nothing
// faulted at the end.
func TestRunTimelineAndHealing(t *testing.T) {
	fake := newFake()
	s := &Scheduler{Ctrl: fake, Nodes: threeNodes(), Seed: 1}
	specs := []domain.FaultSpec{
		{Fault: domain.Fault{Type: domain.FaultKillNode}, At: 50 * time.Millisecond, Duration: 30 * time.Millisecond},
		{Fault: domain.Fault{Type: domain.FaultPartition}, At: 80 * time.Millisecond, Repeat: 40 * time.Millisecond},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	events := s.Run(ctx, specs, t0)

	// Timeline is in non-decreasing offset order.
	for i := 1; i < len(events); i++ {
		if events[i].At < events[i-1].At {
			t.Fatalf("timeline out of order at %d: %v before %v", i, events[i-1], events[i])
		}
	}

	// Scheduled injects: 1 kill + partitions at 80,120,160 = 4 total.
	var injects, heals int
	for _, e := range events {
		switch e.Action {
		case actionInject:
			injects++
		case actionHeal:
			heals++
		}
		if e.Err != "" {
			t.Fatalf("unexpected event error: %v", e)
		}
	}
	if injects != 4 {
		t.Fatalf("inject events = %d, want 4", injects)
	}
	if heals < injects {
		t.Fatalf("heal events = %d, want >= injects (%d)", heals, injects)
	}
	if fake.injected != 4 {
		t.Fatalf("controller injects = %d, want 4", fake.injected)
	}
	if got := fake.outstanding(); got != 0 {
		t.Fatalf("cluster left faulted: %d nodes outstanding", got)
	}
}

// TestNodeSelectionDeterministic checks that a fixed seed yields an identical
// node selection sequence, and a different seed (very likely) differs.
func TestNodeSelectionDeterministic(t *testing.T) {
	specs := []domain.FaultSpec{
		{Fault: domain.Fault{Type: domain.FaultKillNode}, At: 10 * time.Millisecond, Repeat: 10 * time.Millisecond},
	}
	nodesOf := func(seed int64) []string {
		s := &Scheduler{Nodes: threeNodes(), Seed: seed}
		acts := s.plan(specs, 100*time.Millisecond)
		var out []string
		for _, a := range acts {
			if a.inject {
				out = append(out, a.node.ID)
			}
		}
		return out
	}

	a, b := nodesOf(7), nodesOf(7)
	if len(a) == 0 {
		t.Fatal("no injects planned")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("seed 7 not deterministic at %d: %q vs %q", i, a[i], b[i])
		}
	}

	other := nodesOf(99)
	diff := false
	for i := range a {
		if i < len(other) && other[i] != a[i] {
			diff = true
		}
	}
	if !diff {
		t.Fatal("different seed produced identical node selection")
	}
}

// TestExplicitTargetSelected checks an explicit Fault.Target overrides seeded
// selection.
func TestExplicitTargetSelected(t *testing.T) {
	s := &Scheduler{Nodes: threeNodes(), Seed: 1}
	specs := []domain.FaultSpec{
		{Fault: domain.Fault{Type: domain.FaultKillNode, Target: "n2"}, At: 10 * time.Millisecond},
	}
	acts := s.plan(specs, 50*time.Millisecond)
	if len(acts) == 0 || acts[0].node.ID != "n2" {
		t.Fatalf("explicit target not honored: %+v", acts)
	}
}

// TestCancelHealsOutstanding verifies that cancelling ctx heals outstanding
// faults and that Run returns promptly rather than blocking for the fault's
// (10s) duration.
func TestCancelHealsOutstanding(t *testing.T) {
	fake := newFake()
	s := &Scheduler{Ctrl: fake, Nodes: threeNodes(), Seed: 1}
	specs := []domain.FaultSpec{
		{Fault: domain.Fault{Type: domain.FaultKillNode}, At: 10 * time.Millisecond, Duration: 10 * time.Second},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	done := make(chan []Event, 1)
	t0 := time.Now()
	go func() { done <- s.Run(ctx, specs, t0) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after ctx cancel")
	}
	if fake.injected != 1 {
		t.Fatalf("injects = %d, want 1", fake.injected)
	}
	if got := fake.outstanding(); got != 0 {
		t.Fatalf("cluster left faulted after cancel: %d outstanding", got)
	}
}
