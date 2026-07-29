// Package emit is the run-results store: it owns the on-disk directory layout
// for a single run and the atomic writers/readers for that run's artifacts
// (manifest, summary, verification report, and the latency time-series in both
// NDJSON and Parquet form). It depends only on core/domain, the Go standard
// library, and github.com/parquet-go/parquet-go.
package emit

import (
	"os"
	"path/filepath"

	"github.com/proofload/proofload/core/domain"
)

// Paths locates every artifact for one run under
// <base>/results/<target>/<runID>/. Each field is an absolute-or-relative file
// path derived from that run directory; Dir is the directory itself.
type Paths struct {
	Dir               string
	Manifest          string
	Summary           string
	TimeseriesNDJSON  string
	TimeseriesParquet string
	HLog              string
	VerifyJSON        string
	ServerStats       string
}

// Layout derives the canonical set of artifact paths for one run. It performs
// no I/O; call EnsureDir (or any writer) to create the directory on disk.
func Layout(base, target string, id domain.RunID) Paths {
	dir := filepath.Join(base, "results", target, string(id))
	return Paths{
		Dir:               dir,
		Manifest:          filepath.Join(dir, "manifest.json"),
		Summary:           filepath.Join(dir, "summary.json"),
		TimeseriesNDJSON:  filepath.Join(dir, "timeseries.ndjson"),
		TimeseriesParquet: filepath.Join(dir, "timeseries.parquet"),
		HLog:              filepath.Join(dir, "latency.hlog"),
		VerifyJSON:        filepath.Join(dir, "verify.json"),
		ServerStats:       filepath.Join(dir, "server_stats.json"),
	}
}

// EnsureDir creates the run directory (and any missing parents) if absent.
func (p Paths) EnsureDir() error {
	return os.MkdirAll(p.Dir, 0o755)
}
