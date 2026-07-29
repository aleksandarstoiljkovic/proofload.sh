package compose

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// dockerAvailable reports whether the docker CLI can be reached. Integration
// tests gate on this so the default `go test` never requires a daemon.
func dockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}

// runDocker runs `docker <args...>` and returns trimmed stdout, wrapping any
// error with the combined output for diagnosis.
func runDocker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s",
			strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// composeArgs prefixes the compose subcommand with the project name and file.
func composeArgs(project, file string, rest ...string) []string {
	return append([]string{"compose", "-p", project, "-f", file}, rest...)
}

// waitHealthy polls each container until healthy (or running, when it has no
// healthcheck), returning an error if the deadline passes first.
func waitHealthy(ctx context.Context, m projectModel, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pending, err := unhealthy(ctx, m)
		if err == nil && len(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for: %s",
				timeout, strings.Join(pending, ", "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// unhealthy returns the container names not yet ready.
func unhealthy(ctx context.Context, m projectModel) ([]string, error) {
	var pending []string
	for _, s := range m.Services {
		ok, err := containerReady(ctx, s.ContainerName)
		if err != nil || !ok {
			pending = append(pending, s.ContainerName)
		}
	}
	return pending, nil
}

// containerReady reports readiness via docker inspect: healthy when a
// healthcheck exists, otherwise simply running.
func containerReady(ctx context.Context, name string) (bool, error) {
	const format = `{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}`
	out, err := runDocker(ctx, "inspect", "--format", format, name)
	if err != nil {
		return false, err
	}
	return out == "healthy" || out == "running", nil
}

// resolvePort returns the host port docker published for a service's container
// port, falling back to the model's assigned host port when the query fails.
func resolvePort(ctx context.Context, project, file string, s serviceModel) int {
	out, err := runDocker(ctx, composeArgs(project, file,
		"port", s.Key, fmt.Sprint(s.ContainerPort))...)
	if err != nil {
		return s.HostPort
	}
	if i := strings.LastIndex(out, ":"); i >= 0 {
		if p := parsePort(out[i+1:]); p > 0 {
			return p
		}
	}
	return s.HostPort
}

// psEntry is the subset of `docker compose ps --format json` we consume.
type psEntry struct {
	Name    string `json:"Name"`
	Service string `json:"Service"`
	State   string `json:"State"`
	Health  string `json:"Health"`
}

// parsePS decodes `docker compose ps --format json` output, tolerating both the
// newline-delimited object stream and the legacy single JSON array.
func parsePS(raw string) ([]psEntry, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "[") {
		var arr []psEntry
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			return nil, err
		}
		return arr, nil
	}
	var out []psEntry
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var e psEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// parsePort parses a decimal port, returning 0 on any garbage.
func parsePort(s string) int {
	s = strings.TrimSpace(s)
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}
