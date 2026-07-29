package config

import (
	"fmt"

	"github.com/proofload/proofload/core/domain"
)

// workloadFile mirrors the on-disk workload.yaml schema. Enum-like fields are
// read as plain strings here and validated when mapped to domain types.
type workloadFile struct {
	Name        string `koanf:"name"`
	Mode        string `koanf:"mode"`
	KeySpace    int64  `koanf:"key_space"`
	KeyDist     string `koanf:"key_dist"`
	ValueSize   int    `koanf:"value_size"`
	VerifyModel string `koanf:"verify_model"`
	Operations  []struct {
		Type   string `koanf:"type"`
		Weight int    `koanf:"weight"`
	} `koanf:"operations"`
	Params map[string]any `koanf:"params"`
}

// validModes, validDists, and validModels enumerate the accepted enum strings so
// an unknown value fails loudly instead of silently producing a zero domain enum.
var (
	validModes = map[string]domain.RunMode{
		string(domain.ModePerformance): domain.ModePerformance,
		string(domain.ModeCorrectness): domain.ModeCorrectness,
	}
	validDists = map[string]domain.KeyDistribution{
		string(domain.DistUniform):    domain.DistUniform,
		string(domain.DistZipfian):    domain.DistZipfian,
		string(domain.DistLatest):     domain.DistLatest,
		string(domain.DistSequential): domain.DistSequential,
	}
	validModels = map[string]domain.VerifyModel{
		string(domain.VerifyNone):           domain.VerifyNone,
		string(domain.VerifyReconciliation): domain.VerifyReconciliation,
		string(domain.VerifyRegister):       domain.VerifyRegister,
		string(domain.VerifyListAppend):     domain.VerifyListAppend,
		string(domain.VerifyKafkaLog):       domain.VerifyKafkaLog,
	}
)

// LoadWorkload parses workload.yaml at path into a validated domain.Workload.
func LoadWorkload(path string) (domain.Workload, error) {
	var wf workloadFile
	if err := loadYAML(path, &wf); err != nil {
		return domain.Workload{}, err
	}
	return mapWorkload(wf)
}

// mapWorkload converts a parsed workloadFile into a domain.Workload, validating
// every enum-like field.
func mapWorkload(wf workloadFile) (domain.Workload, error) {
	mode, ok := validModes[wf.Mode]
	if !ok {
		return domain.Workload{}, fmt.Errorf("invalid mode %q", wf.Mode)
	}
	dist, ok := validDists[wf.KeyDist]
	if !ok {
		return domain.Workload{}, fmt.Errorf("invalid key_dist %q", wf.KeyDist)
	}
	model, ok := validModels[wf.VerifyModel]
	if !ok {
		return domain.Workload{}, fmt.Errorf("invalid verify_model %q", wf.VerifyModel)
	}

	ops := make([]domain.OpSpec, 0, len(wf.Operations))
	for _, op := range wf.Operations {
		ops = append(ops, domain.OpSpec{Type: domain.OpType(op.Type), Weight: op.Weight})
	}

	return domain.Workload{
		Name:        wf.Name,
		Mode:        mode,
		Operations:  ops,
		KeySpace:    wf.KeySpace,
		KeyDist:     dist,
		ValueSize:   wf.ValueSize,
		VerifyModel: model,
		Params:      wf.Params,
	}, nil
}
