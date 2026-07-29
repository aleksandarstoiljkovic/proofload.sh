// Package config loads and layers proofload's declarative configuration.
//
// It parses the two fixed YAML schemas a target publishes — target.yaml
// (image, defaults, cluster, capabilities) and workload.yaml (the weighted
// operation mix) — and resolves them, together with global defaults, process
// environment, and explicit CLI overrides, into the pure domain values the rest
// of the harness consumes. It is the only package that touches YAML or env vars;
// everything downstream sees resolved domain types.
package config

import (
	"fmt"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// keyDelim separates nested keys inside a koanf instance. The values are flat,
// so the choice only matters for parsing nested YAML maps.
const keyDelim = "."

// loadYAML parses a single YAML file into dest using koanf's `koanf` struct tags.
func loadYAML(path string, dest any) error {
	k := koanf.New(keyDelim)
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := k.Unmarshal("", dest); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
