package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/nemesis"
)

// nemesisRun owns fault injection for one run: it drives the scheduler
// concurrently with the load and always heals on stop.
type nemesisRun struct {
	sched    *nemesis.Scheduler
	specs    []domain.FaultSpec
	cancel   context.CancelFunc
	done     chan struct{}
	timeline []nemesis.Event
	started  bool
	stopped  bool
}

// newNemesis builds the fault runner from --fault, or a no-op when unset. Fault
// injection needs nodes with control endpoints, which only a provisioned cluster
// provides.
func newNemesis(f *runFlags, e Engine, r config.Resolved) (*nemesisRun, error) {
	if f.fault == "" {
		return &nemesisRun{}, nil
	}
	specs, err := loadFaultSchedule(f.fault)
	if err != nil {
		return nil, fmt.Errorf("load fault schedule: %w", err)
	}
	nodes := r.Driver.Cluster.Nodes
	if len(nodes) == 0 {
		return nil, fmt.Errorf("--fault needs cluster nodes with control endpoints; run with --provision")
	}
	script := filepath.Join("targets", e.Name, "faults", "fault.sh")
	if _, err := os.Stat(script); err != nil {
		return nil, fmt.Errorf("fault script %s: %w", script, err)
	}
	ctrl := &nemesis.ScriptController{ScriptPath: script}
	return &nemesisRun{
		sched: &nemesis.Scheduler{Ctrl: ctrl, Nodes: nodes, Seed: f.seed},
		specs: specs,
	}, nil
}

// begin starts the scheduler goroutine, aligning fault offsets to the measure
// phase start. It is a no-op when no schedule was configured.
func (n *nemesisRun) begin(measureStart time.Time) {
	if n == nil || n.sched == nil || n.started {
		return
	}
	n.started = true
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan struct{})
	go func() {
		defer close(n.done)
		n.timeline = n.sched.Run(ctx, n.specs, measureStart)
	}()
}

// stop cancels the scheduler (which heals every outstanding fault) and waits for
// the timeline. Idempotent.
func (n *nemesisRun) stop() {
	if n == nil || !n.started || n.stopped {
		return
	}
	n.stopped = true
	n.cancel()
	<-n.done
}

// persist writes the fault timeline into the run directory.
func (n *nemesisRun) persist(dir string) {
	if n == nil || !n.started || len(n.timeline) == 0 {
		return
	}
	b, err := json.MarshalIndent(n.timeline, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, "faults.json"), b, 0o644)
	fmt.Printf("  faults       %d events (targets/.../faults.json)\n", len(n.timeline))
}

// faultDTO mirrors one entry of a fault schedule YAML file.
type faultDTO struct {
	Type     string         `koanf:"type"`
	Target   string         `koanf:"target"`
	At       string         `koanf:"at"`
	Duration string         `koanf:"duration"`
	Repeat   string         `koanf:"repeat"`
	Params   map[string]any `koanf:"params"`
}

// loadFaultSchedule parses a YAML file of the form:
//
//	faults:
//	  - {type: kill-node, at: 5s, duration: 8s, repeat: 0s, target: ""}
//	  - {type: network-partition, at: 15s, duration: 5s}
func loadFaultSchedule(path string) ([]domain.FaultSpec, error) {
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, err
	}
	var dto struct {
		Faults []faultDTO `koanf:"faults"`
	}
	if err := k.Unmarshal("", &dto); err != nil {
		return nil, err
	}
	specs := make([]domain.FaultSpec, 0, len(dto.Faults))
	for i, d := range dto.Faults {
		at, err := parseDurOrZero(d.At)
		if err != nil {
			return nil, fmt.Errorf("fault %d at: %w", i, err)
		}
		dur, err := parseDurOrZero(d.Duration)
		if err != nil {
			return nil, fmt.Errorf("fault %d duration: %w", i, err)
		}
		rep, err := parseDurOrZero(d.Repeat)
		if err != nil {
			return nil, fmt.Errorf("fault %d repeat: %w", i, err)
		}
		specs = append(specs, domain.FaultSpec{
			Fault:    domain.Fault{Type: domain.FaultType(d.Type), Target: d.Target, Params: d.Params},
			At:       at,
			Duration: dur,
			Repeat:   rep,
		})
	}
	return specs, nil
}

func parseDurOrZero(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
