// Package compose implements the provision.Provisioner port on top of the
// `docker compose` CLI. It renders a deterministic docker-compose project for a
// domain.Topology, brings it up, waits for health, and resolves the published
// host ports into a domain.ClusterSpec whose nodes carry Docker control
// endpoints for later fault injection.
//
// It shells out to `docker` via os/exec and hand-renders YAML with
// text/template so it needs no third-party YAML dependency.
package compose

import (
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/proofload/proofload/core/domain"
)

// kv is a single, order-stable environment entry.
type kv struct{ K, V string }

// health is a container healthcheck rendered into the compose file.
type health struct {
	Test        []string
	Interval    string
	Timeout     string
	Retries     int
	StartPeriod string
}

// serviceModel is one rendered compose service (one cluster node).
type serviceModel struct {
	Key           string
	Image         string
	ContainerName string
	Role          domain.NodeRole
	HostPort      int
	ContainerPort int
	Env           []kv
	Health        *health
	Volume        string
	VolumePath    string
	CPULimit      string
	MemLimit      string
}

// projectModel is the full compose project for one topology.
type projectModel struct {
	Project  string // compose project name: proofload-<name>
	Services []serviceModel
	Volumes  []string
}

// projectName returns the compose -p project name for a topology.
func projectName(t domain.Topology) string { return "proofload-" + t.Name }

// buildModel turns a Topology into a deterministic projectModel. It is the
// single source of truth shared by rendering, bring-up and Nodes parsing.
func buildModel(t domain.Topology) projectModel {
	hook := hookFor(t.Target)
	n := t.Nodes
	if n <= 0 {
		n = 1
	}
	m := projectModel{Project: projectName(t)}
	for i := 0; i < n; i++ {
		m.Services = append(m.Services, buildService(t, hook, i))
	}
	for _, s := range m.Services {
		if s.Volume != "" {
			m.Volumes = append(m.Volumes, s.Volume)
		}
	}
	return m
}

// buildService builds the i-th node's service model.
func buildService(t domain.Topology, hook targetHook, i int) serviceModel {
	name := t.Name + "-" + strconv.Itoa(i)
	s := serviceModel{
		Key:           name,
		Image:         resolveImage(t, hook),
		ContainerName: name,
		Role:          roleFor(hook, i),
		HostPort:      hook.basePort + i,
		ContainerPort: hook.containerPort,
		Env:           sortedEnv(hook.env),
		Health:        hook.health,
		CPULimit:      t.Resources.CPU,
		MemLimit:      t.Resources.Memory,
	}
	if hook.volumePath != "" {
		s.Volume = name + "-data"
		s.VolumePath = hook.volumePath
	}
	return s
}

// resolveImage prefers the topology's explicit image, falling back to the
// per-target default, and appends :version when set.
func resolveImage(t domain.Topology, hook targetHook) string {
	img := t.Image
	if img == "" {
		img = hook.defaultImage
	}
	if img == "" {
		img = t.Target // last resort: treat target as an image name
	}
	if t.Version != "" {
		return img + ":" + t.Version
	}
	return img
}

// roleFor assigns node 0 as primary and the rest as replicas for known
// replicating targets; unknown targets are generic nodes.
func roleFor(hook targetHook, i int) domain.NodeRole {
	if !hook.known {
		return domain.RoleGeneric
	}
	if i == 0 {
		return domain.RolePrimary
	}
	return domain.RoleReplica
}

// sortedEnv returns env entries in stable key order for deterministic output.
func sortedEnv(env []kv) []kv {
	if len(env) == 0 {
		return nil
	}
	out := append([]kv(nil), env...)
	sort.Slice(out, func(a, b int) bool { return out[a].K < out[b].K })
	return out
}

// render renders the projectModel into a docker-compose.yml document.
func render(m projectModel) (string, error) {
	var b strings.Builder
	if err := composeTmpl.Execute(&b, m); err != nil {
		return "", err
	}
	return b.String(), nil
}

var composeTmpl = template.Must(template.New("compose").Parse(composeTemplate))

const composeTemplate = `name: {{.Project}}
services:
{{- range .Services}}
  {{.Key}}:
    image: {{.Image}}
    container_name: {{.ContainerName}}
    ports:
      - "{{.HostPort}}:{{.ContainerPort}}"
{{- if .Env}}
    environment:
{{- range .Env}}
      {{.K}}: "{{.V}}"
{{- end}}
{{- end}}
{{- if .Health}}
    healthcheck:
      test: [{{range $i, $e := .Health.Test}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
      interval: {{.Health.Interval}}
      timeout: {{.Health.Timeout}}
      retries: {{.Health.Retries}}
      start_period: {{.Health.StartPeriod}}
{{- end}}
{{- if .Volume}}
    volumes:
      - {{.Volume}}:{{.VolumePath}}
{{- end}}
{{- if or .CPULimit .MemLimit}}
    deploy:
      resources:
        limits:
{{- if .CPULimit}}
          cpus: "{{.CPULimit}}"
{{- end}}
{{- if .MemLimit}}
          memory: {{.MemLimit}}
{{- end}}
{{- end}}
{{- end}}
{{- if .Volumes}}
volumes:
{{- range .Volumes}}
  {{.}}:
{{- end}}
{{- end}}
`
