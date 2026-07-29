package nemesis

import (
	"context"
	"sort"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// healTimeout bounds the best-effort cleanup that runs after the measure window
// (or a ctx cancel), using a fresh context so cancellation cannot skip it.
const healTimeout = 30 * time.Second

// Event is one entry in the fault timeline. At is the offset from the run start
// T0; Action is "inject" or "heal"; Err is empty on success.
type Event struct {
	At     time.Duration    `json:"at"`
	Action string           `json:"action"`
	Type   domain.FaultType `json:"type,omitempty"`
	Node   string           `json:"node"`
	Err    string           `json:"err,omitempty"`
}

// Scheduler drives a FaultController over a measure window, injecting and
// healing faults on schedule and recording the resulting timeline. Node
// selection is seeded (Seed) so runs are reproducible.
type Scheduler struct {
	Ctrl  driver.FaultController
	Nodes []domain.Node
	Seed  int64
}

// action is one planned inject or heal at a fixed offset from T0.
type action struct {
	at     time.Duration
	inject bool
	ft     domain.FaultType
	node   domain.Node
	params map[string]any
}

// Run executes specs relative to t0, blocking until the measure window ends
// (the ctx deadline, or the last scheduled action when ctx has no deadline) or
// ctx is cancelled. On return every injected fault has been healed, even on
// cancellation: outstanding faults are healed with a fresh context. The returned
// slice is the fault timeline in execution order.
func (s *Scheduler) Run(ctx context.Context, specs []domain.FaultSpec, t0 time.Time) []Event {
	window := s.window(ctx, specs, t0)
	acts := s.plan(specs, window)

	events := make([]Event, 0, len(acts))
	outstanding := make(map[string]domain.Node)

	for _, a := range acts {
		if !waitUntil(ctx, t0.Add(a.at)) {
			break // window ended or ctx cancelled: stop scheduling
		}
		if a.inject {
			err := s.Ctrl.Inject(ctx, domain.Fault{Type: a.ft, Target: a.node.ID, Params: a.params}, a.node)
			events = append(events, Event{At: a.at, Action: actionInject, Type: a.ft, Node: a.node.ID, Err: errStr(err)})
			if err == nil {
				outstanding[a.node.ID] = a.node
			}
			continue
		}
		err := s.Ctrl.Heal(ctx, a.node)
		events = append(events, Event{At: a.at, Action: actionHeal, Node: a.node.ID, Err: errStr(err)})
		delete(outstanding, a.node.ID)
	}

	return s.healAll(outstanding, events, t0)
}

// plan expands specs into a time-ordered action list. Repeats are unrolled while
// the occurrence offset is below window (repeats need a bounded window). Node
// selection happens here, in deterministic (offset, spec-index) order, so the
// chosen nodes are independent of timer jitter at run time.
func (s *Scheduler) plan(specs []domain.FaultSpec, window time.Duration) []action {
	type occ struct {
		at   time.Duration
		idx  int
		spec domain.FaultSpec
	}
	var occs []occ
	for i, sp := range specs {
		for at := sp.At; ; at += sp.Repeat {
			if window > 0 && at >= window {
				break
			}
			occs = append(occs, occ{at: at, idx: i, spec: sp})
			if sp.Repeat <= 0 || window <= 0 {
				break
			}
		}
	}
	sort.SliceStable(occs, func(i, j int) bool {
		if occs[i].at != occs[j].at {
			return occs[i].at < occs[j].at
		}
		return occs[i].idx < occs[j].idx
	})

	rng := newPRNG(s.Seed)
	acts := make([]action, 0, len(occs)*2)
	for _, o := range occs {
		node := s.pick(o.spec.Fault, rng)
		acts = append(acts, action{at: o.at, inject: true, ft: o.spec.Fault.Type, node: node, params: o.spec.Fault.Params})
		if o.spec.Duration > 0 {
			acts = append(acts, action{at: o.at + o.spec.Duration, inject: false, node: node})
		}
	}
	sort.SliceStable(acts, func(i, j int) bool { return acts[i].at < acts[j].at })
	return acts
}

// pick chooses the node a fault targets: the explicit Target if set (falling
// back to a bare Node with that ID if unknown), otherwise a seeded pseudo-random
// node from Nodes.
func (s *Scheduler) pick(f domain.Fault, rng *prng) domain.Node {
	if f.Target != "" {
		for _, n := range s.Nodes {
			if n.ID == f.Target {
				return n
			}
		}
		return domain.Node{ID: f.Target}
	}
	if len(s.Nodes) == 0 {
		return domain.Node{}
	}
	return s.Nodes[rng.intn(len(s.Nodes))]
}

// window returns the measure-window length relative to t0: the ctx deadline when
// present, else the last scheduled action (At+Duration) so a deadline-less run
// still terminates after its final action.
func (s *Scheduler) window(ctx context.Context, specs []domain.FaultSpec, t0 time.Time) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		if d := dl.Sub(t0); d > 0 {
			return d
		}
		return 0
	}
	var max time.Duration
	for _, sp := range specs {
		if end := sp.At + sp.Duration; end > max {
			max = end
		}
	}
	return max
}

// healAll heals every still-outstanding fault with a fresh, bounded context so
// that a cancelled run never leaves the cluster faulted. Nodes are healed in
// sorted ID order for a deterministic tail on the timeline.
func (s *Scheduler) healAll(outstanding map[string]domain.Node, events []Event, t0 time.Time) []Event {
	if len(outstanding) == 0 {
		return events
	}
	ids := make([]string, 0, len(outstanding))
	for id := range outstanding {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ctx, cancel := context.WithTimeout(context.Background(), healTimeout)
	defer cancel()
	for _, id := range ids {
		err := s.Ctrl.Heal(ctx, outstanding[id])
		events = append(events, Event{At: time.Since(t0), Action: actionHeal, Node: id, Err: errStr(err)})
	}
	return events
}

// waitUntil blocks until target or ctx is done. It reports true if target was
// reached, false if ctx ended first.
func waitUntil(ctx context.Context, target time.Time) bool {
	d := time.Until(target)
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
