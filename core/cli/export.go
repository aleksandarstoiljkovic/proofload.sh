package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/export"
)

// exportMetrics pushes the run's time series to a TSDB when --export kind=url is
// set. Failures are reported but never abort the run — the exporter is optional.
func exportMetrics(f *runFlags, r config.Resolved, summary domain.RunResult, snaps []domain.LatencySnapshot) {
	if f.export == "" {
		return
	}
	kind, url, ok := strings.Cut(f.export, "=")
	if !ok {
		fmt.Printf("  (export skipped: expected kind=url, got %q)\n", f.export)
		return
	}
	exp, err := export.New(kind, url)
	if err != nil {
		fmt.Printf("  (export skipped: %v)\n", err)
		return
	}
	defer exp.Close()
	labels := map[string]string{
		"target":   r.Manifest.Target,
		"workload": r.Manifest.Workload,
		"run_id":   string(summary.RunID),
	}
	if err := exp.Export(context.Background(), snaps, labels); err != nil {
		fmt.Printf("  (export failed: %v)\n", err)
		return
	}
	fmt.Printf("  exported     %d snapshots to %s (%s)\n", len(snaps), url, kind)
}
