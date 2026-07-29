package config

import (
	"fmt"
	"time"
)

// Defaults are a target's suggested run parameters, overridable per run.
type Defaults struct {
	Connections int
	Warmup      time.Duration
	Duration    time.Duration
}

// ClusterConfig is a target's declared cluster shape and supported consistency
// levels, as published in target.yaml.
type ClusterConfig struct {
	Backend           string
	Nodes             int
	ReplicationFactor int
	Consistency       []string
}

// Capabilities lists the verification models and fault types a target supports.
type Capabilities struct {
	VerifyModels []string
	Faults       []string
}

// TargetConfig is the parsed, duration-resolved view of a target.yaml file.
type TargetConfig struct {
	Target       string
	Image        string
	Version      string
	Defaults     Defaults
	Cluster      ClusterConfig
	Capabilities Capabilities
}

// targetFile mirrors the on-disk target.yaml schema. Durations are read as
// strings ("60s") and parsed into time.Duration when mapped to TargetConfig.
type targetFile struct {
	Target   string `koanf:"target"`
	Image    string `koanf:"image"`
	Version  string `koanf:"version"`
	Defaults struct {
		Connections int    `koanf:"connections"`
		Warmup      string `koanf:"warmup"`
		Duration    string `koanf:"duration"`
	} `koanf:"defaults"`
	Cluster struct {
		Backend           string   `koanf:"backend"`
		Nodes             int      `koanf:"nodes"`
		ReplicationFactor int      `koanf:"replication_factor"`
		Consistency       []string `koanf:"consistency"`
	} `koanf:"cluster"`
	Capabilities struct {
		VerifyModels []string `koanf:"verify_models"`
		Faults       []string `koanf:"faults"`
	} `koanf:"capabilities"`
}

// LoadTarget parses target.yaml at path into a TargetConfig, resolving the
// warmup/duration strings into time.Duration values.
func LoadTarget(path string) (TargetConfig, error) {
	var tf targetFile
	if err := loadYAML(path, &tf); err != nil {
		return TargetConfig{}, err
	}
	return mapTarget(tf)
}

// mapTarget converts a parsed targetFile into a TargetConfig.
func mapTarget(tf targetFile) (TargetConfig, error) {
	warmup, err := parseDuration(tf.Defaults.Warmup, "warmup")
	if err != nil {
		return TargetConfig{}, err
	}
	duration, err := parseDuration(tf.Defaults.Duration, "duration")
	if err != nil {
		return TargetConfig{}, err
	}

	return TargetConfig{
		Target:  tf.Target,
		Image:   tf.Image,
		Version: tf.Version,
		Defaults: Defaults{
			Connections: tf.Defaults.Connections,
			Warmup:      warmup,
			Duration:    duration,
		},
		Cluster: ClusterConfig{
			Backend:           tf.Cluster.Backend,
			Nodes:             tf.Cluster.Nodes,
			ReplicationFactor: tf.Cluster.ReplicationFactor,
			Consistency:       tf.Cluster.Consistency,
		},
		Capabilities: Capabilities{
			VerifyModels: tf.Capabilities.VerifyModels,
			Faults:       tf.Capabilities.Faults,
		},
	}, nil
}

// parseDuration parses a duration string, treating empty as zero. field names
// the setting for error messages.
func parseDuration(s, field string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, s, err)
	}
	return d, nil
}
