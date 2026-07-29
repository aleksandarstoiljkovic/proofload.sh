package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/emit"
	"github.com/proofload/proofload/core/metrics"
	reportlib "github.com/proofload/proofload/core/report"
	"github.com/proofload/proofload/core/runner"
)

// writeArtifacts finalizes the manifest, computes the summary, and writes the
// per-run artifacts (manifest, summary, time series, hlog). It returns the
// resolved paths and the summary so the caller can run verification (which adds
// verify.json) before the HTML report is rendered.
func writeArtifacts(e Engine, r config.Resolved, report runner.RunReport, rec *metrics.Recorder, snaps []domain.LatencySnapshot, start time.Time, base string) (emit.Paths, domain.RunResult, error) {
	runID := domain.RunID(fmt.Sprintf("%s-%s-%03d", e.Name,
		start.UTC().Format("20060102-150405"), start.Nanosecond()/1e6))

	summary := rec.Snapshot(domain.PhaseMeasure, report.Duration)
	summary.RunID = runID
	summary.ClientBound = report.ClientBound
	if report.Duration > 0 {
		summary.Throughput = float64(summary.Total) / report.Duration.Seconds()
	}

	manifest := r.Manifest
	manifest.RunID = runID
	manifest.EngineVersion = version
	manifest.Client = clientInfo()
	manifest.CreatedAt = start.UTC()

	p := emit.Layout(base, e.Name, runID)
	if err := p.EnsureDir(); err != nil {
		return p, summary, fmt.Errorf("create results dir: %w", err)
	}
	if err := emit.WriteManifest(p, manifest); err != nil {
		return p, summary, fmt.Errorf("write manifest: %w", err)
	}
	if err := emit.WriteSummary(p, summary); err != nil {
		return p, summary, fmt.Errorf("write summary: %w", err)
	}
	if err := emit.WriteTimeseriesNDJSON(p, snaps); err != nil {
		return p, summary, fmt.Errorf("write timeseries ndjson: %w", err)
	}
	if err := emit.WriteTimeseriesParquet(p, snaps); err != nil {
		return p, summary, fmt.Errorf("write timeseries parquet: %w", err)
	}
	if err := writeHLog(p, rec, start); err != nil {
		return p, summary, fmt.Errorf("write hlog: %w", err)
	}
	return p, summary, nil
}

func writeHLog(p emit.Paths, rec *metrics.Recorder, start time.Time) error {
	f, err := os.Create(p.HLog)
	if err != nil {
		return err
	}
	defer f.Close()
	return metrics.WriteHLog(f, rec, start)
}

// finalize renders the HTML report (after verify.json exists) and prints the
// human-readable summary.
func finalize(e Engine, manifest domain.Manifest, s domain.RunResult, report runner.RunReport, p emit.Paths, vr *domain.VerifyReport) {
	reportPath, rerr := reportlib.WriteReport(p.Dir)
	printSummary(e, manifest, s, report, p.Dir)
	if vr != nil {
		printVerdict(*vr)
	}
	if rerr != nil {
		fmt.Printf("  (report generation skipped: %v)\n\n", rerr)
	} else {
		fmt.Printf("  report       %s\n\n", reportPath)
	}
}

// printSummary renders a concise result block to stdout.
func printSummary(e Engine, m domain.Manifest, s domain.RunResult, report runner.RunReport, dir string) {
	fmt.Printf("\nproofload · %s · %s\n", e.Name, m.Workload)
	if m.TestType != "" {
		fmt.Printf("  test         %s\n", m.TestType)
	}
	fmt.Printf("  mode         %s\n", m.Mode)
	fmt.Printf("  connections  %d\n", m.Connections)
	fmt.Printf("  duration     %s\n", report.Duration.Round(time.Millisecond))
	fmt.Printf("  throughput   %.0f req/s\n", s.Throughput)
	if report.Records > report.Total && report.Duration > 0 {
		fmt.Printf("  records      %d (%.0f rec/s — batched)\n",
			report.Records, float64(report.Records)/report.Duration.Seconds())
	}
	fmt.Printf("  ops / errors %d / %d\n", s.Total, s.Errors)
	fmt.Printf("  p50 %.2fms  p95 %.2fms  p99 %.2fms  p99.9 %.2fms  max %.2fms\n",
		s.Overall.P50, s.Overall.P95, s.Overall.P99, s.Overall.P999, s.Overall.Max)
	if s.ClientBound {
		fmt.Printf("  ⚠ CLIENT-BOUND: the load generator (not the target) was the limiter — "+
			"mean lateness %s > mean service %s. Add --connections or scale out with --workers.\n",
			report.MeanLateness.Round(time.Microsecond), report.MeanService.Round(time.Microsecond))
	}
	fmt.Printf("  results      %s\n", dir)
}

// printVerdict renders the correctness outcome.
func printVerdict(v domain.VerifyReport) {
	mark := "✓"
	if v.Verdict != domain.VerdictPass {
		mark = "✗"
	}
	fmt.Printf("  %s verify      %s (%s): checked=%d lost=%d dup=%d ordering=%d anomalies=%d\n",
		mark, v.Verdict, v.Model, v.Checked, v.Lost, v.Duplicated, v.OrderingViol, len(v.Anomalies))
	for i, a := range v.Anomalies {
		if i >= 3 {
			fmt.Printf("      … and %d more anomalies\n", len(v.Anomalies)-3)
			break
		}
		fmt.Printf("      - %s: %s\n", a.Kind, a.Detail)
	}
}
