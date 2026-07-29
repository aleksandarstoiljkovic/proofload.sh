package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// clusterAvailable reports whether a kubectl-reachable cluster is up.
// Integration tests gate on this so the default `go test` never requires a
// cluster.
func clusterAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "kubectl", "cluster-info").Run() == nil
}

// runKubectl runs `kubectl <args...>` and returns trimmed stdout, wrapping any
// error with the combined output for diagnosis.
func runKubectl(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("kubectl %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// rolloutStatus blocks until the StatefulSet's rollout completes or the timeout
// elapses.
func rolloutStatus(ctx context.Context, m manifestModel, timeout time.Duration) error {
	_, err := runKubectl(ctx,
		"rollout", "status", "statefulset/"+m.Name,
		"-n", m.Namespace,
		"--timeout="+timeout.String(),
	)
	if err != nil {
		return fmt.Errorf("rollout status: %w", err)
	}
	return nil
}

// podItem is the subset of `kubectl get pods -o json` we consume.
type podItem struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// podList is the top level of `kubectl get pods -o json`.
type podList struct {
	Items []podItem `json:"items"`
}

// parsePods decodes `kubectl get pods -o json` into its items.
func parsePods(raw string) ([]podItem, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var pl podList
	if err := json.Unmarshal([]byte(raw), &pl); err != nil {
		return nil, err
	}
	return pl.Items, nil
}

// podOrdinal extracts the StatefulSet ordinal from a pod name of the form
// "<statefulset>-<ordinal>", returning -1 when it cannot be parsed.
func podOrdinal(name string) int {
	i := strings.LastIndex(name, "-")
	if i < 0 || i == len(name)-1 {
		return -1
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return -1
	}
	return n
}
