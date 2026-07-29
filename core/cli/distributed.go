package cli

import (
	"context"
	"fmt"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/proofload/proofload/core/cluster"
	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/metrics"
	"github.com/proofload/proofload/core/runner"
	"github.com/spf13/cobra"
)

// coordinate runs the distributed coordinator: it applies the schema once, waits
// for f.workers worker processes to join, then merges their histograms losslessly
// into one aggregate result. Workers are launched separately as `worker` procs.
func coordinate(ctx context.Context, cmd *cobra.Command, e Engine, f *runFlags, r config.Resolved) error {
	if err := e.Driver.Schema(ctx, r.Driver, r.Workload); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	spec := cluster.RunSpec{
		Target: e.Name, Workload: f.workload, Endpoints: r.Driver.Endpoints,
		Consistency: r.Driver.Consistency, Connections: r.Manifest.Connections,
		Warmup: r.Manifest.Warmup, Duration: r.Manifest.Duration,
		RateOpsPerSec: r.Manifest.Rate.OpsPerSec, Seed: f.seed, WorkerCount: f.workers,
	}
	c := cluster.NewCoordinator(f.coordAddr, spec, f.workers)
	out := cmd.OutOrStdout()

	// Serve binds the listener and blocks until all workers submit, so run it in
	// a goroutine; Addr() unblocks once the listener is bound.
	type served struct {
		res cluster.Result
		err error
	}
	ch := make(chan served, 1)
	go func() {
		r, err := c.Serve(ctx)
		ch <- served{r, err}
	}()

	addr := c.Addr()
	fmt.Fprintf(out, "coordinator listening on %s — waiting for %d workers\n", addr, f.workers)
	fmt.Fprintf(out, "  launch each worker with:\n    ./proofload.sh %s worker --coordinator %s\n", e.Name, addr)

	sr := <-ch
	if sr.err != nil {
		return fmt.Errorf("coordinator: %w", sr.err)
	}
	res := sr.res
	merged, report, err := mergeResults(res)
	if err != nil {
		return err
	}
	report.Duration = spec.Duration // aggregate throughput is over the measure window
	start := time.Now()
	p, summary, err := writeArtifacts(e, r, report, merged, nil, start, f.results)
	if err != nil {
		return err
	}
	finalize(e, r.Manifest, summary, report, p, nil)
	return nil
}

// mergeResults losslessly merges every worker's encoded histogram and sums the
// scalar tallies.
func mergeResults(res cluster.Result) (*metrics.Recorder, runner.RunReport, error) {
	if len(res.Histograms) == 0 {
		return nil, runner.RunReport{}, fmt.Errorf("no worker results")
	}
	merged, err := metrics.Decode(res.Histograms[0])
	if err != nil {
		return nil, runner.RunReport{}, fmt.Errorf("decode worker 0: %w", err)
	}
	for i := 1; i < len(res.Histograms); i++ {
		r, err := metrics.Decode(res.Histograms[i])
		if err != nil {
			return nil, runner.RunReport{}, fmt.Errorf("decode worker %d: %w", i, err)
		}
		merged.Merge(r)
	}
	var rep runner.RunReport
	for _, wr := range res.Reports {
		rep.Total += wr.Total
		rep.Errors += wr.Errors
		rep.ClientBound = rep.ClientBound || wr.ClientBound
	}
	return merged, rep, nil
}

// workerCmd is the distributed worker: it joins a coordinator, runs its shard of
// the load synchronized to the start-gun, and submits its encoded histogram.
func workerCmd(e Engine) *cobra.Command {
	var coord string
	var id int
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Join a coordinator and run one shard of a distributed load",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return doWorker(cmd, e, coord, id)
		},
	}
	cmd.Flags().StringVar(&coord, "coordinator", "", "coordinator address host:port")
	cmd.Flags().IntVar(&id, "id", -1, "worker id (-1 = auto-assign)")
	_ = cmd.MarkFlagRequired("coordinator")
	return cmd
}

func doWorker(cmd *cobra.Command, e Engine, coordAddr string, id int) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := cluster.NewWorker(coordAddr, id)
	spec, startAt, err := w.Join(ctx)
	if err != nil {
		return fmt.Errorf("join coordinator: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "worker %d joined; measure starts at %s\n", w.ID(), startAt.Format(time.RFC3339Nano))

	dir, cleanup, err := materialize(e.Assets)
	if err != nil {
		return err
	}
	defer cleanup()
	resolved, err := config.Resolve(config.ResolveOptions{
		TargetPath:   filepath.Join(dir, "target.yaml"),
		WorkloadPath: filepath.Join(dir, "workloads", spec.Workload+".yaml"),
		Overrides:    workerOverrides(spec),
	})
	if err != nil {
		return fmt.Errorf("resolve config: %w", err)
	}

	report, rec, _, _, err := drive(ctx, e, driveParams{
		workload: resolved.Workload, cfg: resolved.Driver, rate: workerRate(spec),
		connections: spec.Connections, warmup: spec.Warmup, duration: spec.Duration,
		seed: spec.Seed, workerID: w.ID(), workerCount: spec.WorkerCount, startAt: startAt,
	})
	if err != nil {
		return err
	}
	blob, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("encode histogram: %w", err)
	}
	if err := w.Submit(ctx, cluster.WorkerReport{
		WorkerID: w.ID(), Total: report.Total, Errors: report.Errors, ClientBound: report.ClientBound,
	}, blob); err != nil {
		return fmt.Errorf("submit results: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "worker %d done: %d ops, %d errors\n", w.ID(), report.Total, report.Errors)
	return nil
}

func workerOverrides(spec cluster.RunSpec) map[string]any {
	o := map[string]any{
		"connections": spec.Connections,
		"duration":    spec.Duration,
		"warmup":      spec.Warmup,
	}
	if len(spec.Endpoints) > 0 {
		o["endpoints"] = spec.Endpoints
	}
	if spec.Consistency != "" {
		o["consistency"] = spec.Consistency
	}
	return o
}

// workerRate splits a fixed aggregate rate evenly across workers; a zero rate
// stays closed-loop (each worker drives to its own max).
func workerRate(spec cluster.RunSpec) domain.RateSpec {
	if spec.RateOpsPerSec <= 0 || spec.WorkerCount <= 0 {
		return domain.RateSpec{}
	}
	return domain.RateSpec{Mode: domain.RateFixed, OpsPerSec: spec.RateOpsPerSec / spec.WorkerCount}
}
