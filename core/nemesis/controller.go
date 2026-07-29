// Package nemesis injects and heals faults during a proofload run. It drives
// each target's faults/fault.sh via a node's control endpoint (k8s/docker/ssh)
// and records a fault timeline correlated with the measure window.
package nemesis

import (
	"context"
	"fmt"
	"os/exec"
	"sort"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// Actions passed as the first positional argument to fault.sh.
const (
	actionInject = "inject"
	actionHeal   = "heal"
)

// ScriptController implements driver.FaultController by exec'ing a target's
// faults/fault.sh. The action, control method, node reference and fault type are
// passed as an argument vector built by buildArgs. Runner is injectable so tests
// can capture the argv without touching the filesystem or a real cluster; when
// nil it defaults to os/exec.
type ScriptController struct {
	ScriptPath string
	Runner     func(ctx context.Context, name string, args ...string) error
}

var _ driver.FaultController = (*ScriptController)(nil)

// Inject applies fault f to target by running `fault.sh inject ...`.
func (c *ScriptController) Inject(ctx context.Context, f domain.Fault, target domain.Node) error {
	return c.run(ctx, buildArgs(actionInject, f.Type, target, f.Params)...)
}

// Heal reverses whatever fault was applied to target by running `fault.sh heal
// ...`. Heal carries no fault type (the FaultController contract does not supply
// one), so fault.sh undoes every applied effect best-effort.
func (c *ScriptController) Heal(ctx context.Context, target domain.Node) error {
	return c.run(ctx, buildArgs(actionHeal, "", target, nil)...)
}

func (c *ScriptController) run(ctx context.Context, args ...string) error {
	runner := c.Runner
	if runner == nil {
		runner = execRunner
	}
	return runner(ctx, c.ScriptPath, args...)
}

// execRunner is the default Runner: it executes name with args and folds any
// captured output into the returned error so failures are diagnosable.
func execRunner(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nemesis: %s %v failed: %w: %s", name, args, err, out)
	}
	return nil
}

// buildArgs builds the argument vector passed to fault.sh. It is pure and
// deterministic (params are emitted in sorted key order) so it can be asserted
// directly in tests.
//
// Contract:
//
//	inject: fault.sh inject --method <m> <refflag> <ref> [--namespace <ns>] \
//	        --type <faultType> [--param key=value ...]
//	heal:   fault.sh heal   --method <m> <refflag> <ref> [--namespace <ns>]
//
// where <refflag> is --pod (k8s), --container (docker), --host (ssh) or --ref
// (fallback), and the node reference comes from Node.Control.Ref.
func buildArgs(action string, ft domain.FaultType, node domain.Node, params map[string]any) []string {
	ep := node.Control
	args := []string{action, "--method", string(ep.Method), refFlag(ep.Method), ep.Ref}
	if ep.Namespace != "" {
		args = append(args, "--namespace", ep.Namespace)
	}
	if action != actionInject {
		return args
	}
	args = append(args, "--type", string(ft))
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--param", fmt.Sprintf("%s=%v", k, params[k]))
	}
	return args
}

// refFlag maps a control method to the flag naming its node reference.
func refFlag(m domain.ControlMethod) string {
	switch m {
	case domain.ControlK8s:
		return "--pod"
	case domain.ControlDocker:
		return "--container"
	case domain.ControlSSH:
		return "--host"
	default:
		return "--ref"
	}
}
