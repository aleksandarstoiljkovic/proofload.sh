package emit

import (
	"encoding/json"
	"os"

	"github.com/proofload/proofload/core/domain"
)

// writeJSON marshals v as indented JSON and writes it atomically to path.
func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, data)
}

// readJSON reads path and unmarshals its JSON contents into v.
func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// WriteManifest persists the run manifest as indented JSON, atomically.
func WriteManifest(p Paths, m domain.Manifest) error {
	return writeJSON(p.Manifest, m)
}

// ReadManifest loads the run manifest written by WriteManifest.
func ReadManifest(p Paths) (domain.Manifest, error) {
	var m domain.Manifest
	err := readJSON(p.Manifest, &m)
	return m, err
}

// WriteSummary persists the aggregate run result as indented JSON, atomically.
func WriteSummary(p Paths, r domain.RunResult) error {
	return writeJSON(p.Summary, r)
}

// ReadSummary loads the aggregate run result written by WriteSummary.
func ReadSummary(p Paths) (domain.RunResult, error) {
	var r domain.RunResult
	err := readJSON(p.Summary, &r)
	return r, err
}

// WriteVerify persists the verification report as indented JSON, atomically.
func WriteVerify(p Paths, v domain.VerifyReport) error {
	return writeJSON(p.VerifyJSON, v)
}

// ReadVerify loads the verification report written by WriteVerify.
func ReadVerify(p Paths) (domain.VerifyReport, error) {
	var v domain.VerifyReport
	err := readJSON(p.VerifyJSON, &v)
	return v, err
}
