package nemesis

import (
	"context"
	"reflect"
	"testing"

	"github.com/proofload/proofload/core/domain"
)

// capture records the argv of the most recent Runner invocation.
type capture struct {
	name string
	args []string
	n    int
}

func (c *capture) run(_ context.Context, name string, args ...string) error {
	c.name = name
	c.args = args
	c.n++
	return nil
}

func k8sNode() domain.Node {
	return domain.Node{ID: "pg-0", Control: domain.ControlEndpoint{
		Method: domain.ControlK8s, Ref: "pg-0", Namespace: "proofload"}}
}

func dockerNode() domain.Node {
	return domain.Node{ID: "pg1", Control: domain.ControlEndpoint{
		Method: domain.ControlDocker, Ref: "pg1"}}
}

func sshNode() domain.Node {
	return domain.Node{ID: "db", Control: domain.ControlEndpoint{
		Method: domain.ControlSSH, Ref: "10.0.0.5"}}
}

// TestInjectArgvByMethodAndType asserts the argv fault.sh receives for every
// control method and each fault type, captured via the injectable Runner.
func TestInjectArgvByMethodAndType(t *testing.T) {
	tests := []struct {
		name string
		node domain.Node
		ft   domain.FaultType
		want []string
	}{
		{
			name: "k8s kill",
			node: k8sNode(),
			ft:   domain.FaultKillNode,
			want: []string{"inject", "--method", "k8s", "--pod", "pg-0", "--namespace", "proofload", "--type", "kill-node"},
		},
		{
			name: "docker pause",
			node: dockerNode(),
			ft:   domain.FaultPauseNode,
			want: []string{"inject", "--method", "docker", "--container", "pg1", "--type", "pause-node"},
		},
		{
			name: "ssh partition",
			node: sshNode(),
			ft:   domain.FaultPartition,
			want: []string{"inject", "--method", "ssh", "--host", "10.0.0.5", "--type", "network-partition"},
		},
		{
			name: "k8s clock-skew",
			node: k8sNode(),
			ft:   domain.FaultClockSkew,
			want: []string{"inject", "--method", "k8s", "--pod", "pg-0", "--namespace", "proofload", "--type", "clock-skew"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cap := &capture{}
			c := &ScriptController{ScriptPath: "fault.sh", Runner: cap.run}
			if err := c.Inject(context.Background(), domain.Fault{Type: tt.ft}, tt.node); err != nil {
				t.Fatalf("Inject: %v", err)
			}
			if cap.name != "fault.sh" {
				t.Fatalf("script = %q, want fault.sh", cap.name)
			}
			if !reflect.DeepEqual(cap.args, tt.want) {
				t.Fatalf("argv =\n  %v\nwant\n  %v", cap.args, tt.want)
			}
		})
	}
}

// TestInjectParamsSorted checks fault params are emitted as sorted --param
// key=value pairs so the argv is deterministic.
func TestInjectParamsSorted(t *testing.T) {
	cap := &capture{}
	c := &ScriptController{ScriptPath: "fault.sh", Runner: cap.run}
	f := domain.Fault{Type: domain.FaultClockSkew, Params: map[string]any{"skew": "300s", "dir": "+"}}
	if err := c.Inject(context.Background(), f, dockerNode()); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	want := []string{"inject", "--method", "docker", "--container", "pg1",
		"--type", "clock-skew", "--param", "dir=+", "--param", "skew=300s"}
	if !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("argv =\n  %v\nwant\n  %v", cap.args, want)
	}
}

// TestHealArgv checks Heal builds a heal argv with no --type and no params.
func TestHealArgv(t *testing.T) {
	cap := &capture{}
	c := &ScriptController{ScriptPath: "fault.sh", Runner: cap.run}
	if err := c.Heal(context.Background(), k8sNode()); err != nil {
		t.Fatalf("Heal: %v", err)
	}
	want := []string{"heal", "--method", "k8s", "--pod", "pg-0", "--namespace", "proofload"}
	if !reflect.DeepEqual(cap.args, want) {
		t.Fatalf("argv =\n  %v\nwant\n  %v", cap.args, want)
	}
}
