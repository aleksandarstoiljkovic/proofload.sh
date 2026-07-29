package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	envprovider "github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// envPrefix scopes which environment variables the loader reads.
const envPrefix = "PROOFLOAD_"

// ResolveOptions are the inputs to a full configuration resolution: file paths
// for each layer plus the highest-precedence CLI-derived values.
type ResolveOptions struct {
	GlobalPath   string
	TargetPath   string
	WorkloadPath string
	Endpoints    []string
	Consistency  string
	Overrides    map[string]any
}

// Resolved is the fully layered configuration handed to the runner. Manifest is
// partially filled here; the lead completes RunID/EngineVersion/GitSHA/Client
// and CreatedAt at run time.
type Resolved struct {
	Workload domain.Workload
	Topology domain.Topology
	Driver   driver.Config
	Manifest domain.Manifest
}

// settings holds the scalar run parameters resolved across every layer.
type settings struct {
	Connections int
	Duration    time.Duration
	Warmup      time.Duration
	Consistency string
	Endpoints   []string
	Rate        domain.RateSpec
}

// Resolve loads every layer and merges them by precedence (lowest to highest):
// global defaults file, target.yaml, workload.yaml, PROOFLOAD_ env vars, and the
// explicit Overrides map. It returns the pure domain values the harness runs on.
func Resolve(o ResolveOptions) (Resolved, error) {
	target, err := LoadTarget(o.TargetPath)
	if err != nil {
		return Resolved{}, err
	}
	workload, err := LoadWorkload(o.WorkloadPath)
	if err != nil {
		return Resolved{}, err
	}
	s, err := resolveSettings(o, target)
	if err != nil {
		return Resolved{}, err
	}

	cluster := domain.ClusterSpec{
		ReplicationFactor: target.Cluster.ReplicationFactor,
		Consistency:       target.Cluster.Consistency,
	}

	return Resolved{
		Workload: workload,
		Topology: buildTopology(target),
		Driver: driver.Config{
			Endpoints:   s.Endpoints,
			Consistency: s.Consistency,
			Cluster:     cluster,
			Params:      workload.Params,
		},
		Manifest: domain.Manifest{
			Target:        target.Target,
			TargetVersion: target.Version,
			Workload:      workload.Name,
			Mode:          workload.Mode,
			Rate:          s.Rate,
			Duration:      s.Duration,
			Warmup:        s.Warmup,
			Connections:   s.Connections,
			Consistency:   s.Consistency,
			Cluster:       cluster,
		},
	}, nil
}

// buildTopology maps a target's declared cluster shape onto a domain.Topology,
// the declarative input a provisioner turns into a live ClusterSpec.
func buildTopology(t TargetConfig) domain.Topology {
	return domain.Topology{
		Name:              t.Target,
		Backend:           domain.Backend(t.Cluster.Backend),
		Target:            t.Target,
		Image:             t.Image,
		Version:           t.Version,
		Nodes:             t.Cluster.Nodes,
		ReplicationFactor: t.Cluster.ReplicationFactor,
	}
}

// resolveSettings merges the scalar run parameters across all layers using a
// single koanf instance; each successive Load/Set overrides the previous.
func resolveSettings(o ResolveOptions, t TargetConfig) (settings, error) {
	k := koanf.New(keyDelim)

	if o.GlobalPath != "" {
		if err := k.Load(file.Provider(o.GlobalPath), yaml.Parser()); err != nil {
			return settings{}, fmt.Errorf("read %s: %w", o.GlobalPath, err)
		}
	}

	_ = k.Load(confmap.Provider(map[string]any{
		"connections": t.Defaults.Connections,
		"duration":    t.Defaults.Duration,
		"warmup":      t.Defaults.Warmup,
	}, keyDelim), nil)
	if len(t.Cluster.Consistency) > 0 {
		_ = k.Set("consistency", t.Cluster.Consistency[0])
	}

	if o.Consistency != "" {
		_ = k.Set("consistency", o.Consistency)
	}
	if len(o.Endpoints) > 0 {
		_ = k.Set("endpoints", o.Endpoints)
	}

	if err := k.Load(envProvider(), nil); err != nil {
		return settings{}, fmt.Errorf("read env: %w", err)
	}
	if len(o.Overrides) > 0 {
		_ = k.Load(confmap.Provider(o.Overrides, keyDelim), nil)
	}

	return readSettings(k), nil
}

// envProvider reads PROOFLOAD_-prefixed variables into the flat key namespace,
// splitting the endpoints list on commas.
func envProvider() *envprovider.Env {
	return envprovider.Provider(keyDelim, envprovider.Opt{
		Prefix: envPrefix,
		TransformFunc: func(key, value string) (string, any) {
			name := strings.ToLower(strings.TrimPrefix(key, envPrefix))
			if name == "endpoints" {
				return name, strings.Split(value, ",")
			}
			return name, value
		},
	})
}

// readSettings extracts the resolved scalar values from the merged koanf.
func readSettings(k *koanf.Koanf) settings {
	s := settings{
		Connections: k.Int("connections"),
		Duration:    k.Duration("duration"),
		Warmup:      k.Duration("warmup"),
		Consistency: k.String("consistency"),
		Endpoints:   k.Strings("endpoints"),
	}
	if ops := k.Int("ops_per_sec"); ops > 0 {
		s.Rate = domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: ops}
	}
	return s
}
