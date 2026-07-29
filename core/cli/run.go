package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/proofload/proofload/core/config"
	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/proofload/proofload/core/emit"
	"github.com/proofload/proofload/core/metrics"
	"github.com/proofload/proofload/core/provision"
	"github.com/proofload/proofload/core/runner"
	"github.com/proofload/proofload/core/schedule"
	"github.com/proofload/proofload/core/workload"
	"github.com/spf13/cobra"
)

type runFlags struct {
	workload    string
	endpoints   []string
	consistency string
	connections int
	duration    time.Duration
	warmup      time.Duration
	rate        int
	seed        int64
	results     string
	provision   string
	verify      string
	export      string
	fault       string
	workers     int
	coordAddr   string
	keep        bool
	test        string
}

func runCmd(e Engine) *cobra.Command {
	var f runFlags
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run a workload against the target and record results",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return doRun(cmd, e, &f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.workload, "workload", "", "workload name (file under workloads/)")
	fl.StringSliceVar(&f.endpoints, "endpoints", nil, "client endpoints host:port (comma-separated)")
	fl.StringVar(&f.consistency, "consistency", "", "consistency/isolation level")
	fl.IntVar(&f.connections, "connections", 0, "concurrent connections (0 = target default)")
	fl.DurationVar(&f.duration, "duration", 0, "measure duration (0 = target default)")
	fl.DurationVar(&f.warmup, "warmup", 0, "warmup duration, discarded (0 = target default)")
	fl.IntVar(&f.rate, "rate", 0, "target ops/sec (0 = closed-loop / max throughput)")
	fl.Int64Var(&f.seed, "seed", 42, "workload RNG seed")
	fl.StringVar(&f.results, "results", ".", "results base directory (a results/ subtree is created under it)")
	fl.StringVar(&f.provision, "provision", "", "provision a cluster via backend (compose|kubernetes) instead of using --endpoints")
	fl.StringVar(&f.verify, "verify", "", "run a correctness check after the run (reconciliation|register|list-append|kafka-log); empty uses the workload's verify_model")
	fl.StringVar(&f.export, "export", "", "also push metrics to a TSDB as kind=url (influx=http://... | pushgateway=http://...)")
	fl.StringVar(&f.fault, "fault", "", "inject faults during the run from a schedule YAML file (requires --provision for node control)")
	fl.IntVar(&f.workers, "workers", 0, "act as a distributed coordinator expecting N worker processes (0 = single-node)")
	fl.StringVar(&f.coordAddr, "coordinator-addr", "127.0.0.1:8677", "address to listen on as a distributed coordinator")
	fl.BoolVar(&f.keep, "keep", false, "leave a provisioned cluster running after the run (default: tear it down)")
	fl.StringVar(&f.test, "test", "", "test type: benchmark (max throughput) | load (constant --rate) | stress (ramp to the knee) | acid (correctness under load) | combined (load+faults+verify). Empty = load if --rate set, else benchmark.")
	_ = cmd.MarkFlagRequired("workload")
	return cmd
}

func doRun(cmd *cobra.Command, e Engine, f *runFlags) error {
	dir, cleanup, err := materialize(e.Assets)
	if err != nil {
		return fmt.Errorf("load assets: %w", err)
	}
	defer cleanup()

	resolved, err := config.Resolve(config.ResolveOptions{
		TargetPath:   filepath.Join(dir, "target.yaml"),
		WorkloadPath: filepath.Join(dir, "workloads", f.workload+".yaml"),
		Overrides:    overrides(cmd, f),
	})
	if err != nil {
		return fmt.Errorf("resolve config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	teardown, err := ensureEndpoints(ctx, cmd, f, e.Name, &resolved)
	if err != nil {
		return err
	}
	defer teardown()

	resolved.Manifest.Seed = f.seed

	tt, err := resolveTestType(f)
	if err != nil {
		return err
	}
	resolved.Manifest.TestType = tt

	if f.workers > 0 {
		return coordinate(ctx, cmd, e, f, resolved)
	}

	if err := e.Driver.Schema(ctx, resolved.Driver, resolved.Workload); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	model := resolveVerifyModel(f, resolved.Workload)
	ver, err := newVerification(model, resolved)
	if err != nil {
		return err
	}
	defer ver.cleanup()

	nem, err := newNemesis(f, e, resolved)
	if err != nil {
		return err
	}
	defer nem.stop()

	report, rec, snaps, start, err := drive(ctx, e, driveParams{
		workload: resolved.Workload, cfg: resolved.Driver, rate: resolved.Manifest.Rate,
		connections: resolved.Manifest.Connections, warmup: resolved.Manifest.Warmup,
		duration: resolved.Manifest.Duration, seed: f.seed, workerCount: 1,
		observer: ver.observer(), nemesis: nem, testType: tt,
	})
	if err != nil {
		return err
	}
	nem.stop() // stop faults, heal the cluster, and collect the timeline

	p, summary, err := writeArtifacts(e, resolved, report, rec, snaps, start, f.results)
	if err != nil {
		return err
	}

	vr, err := ver.run(ctx, e, resolved, p)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	if vr != nil {
		if err := emit.WriteVerify(p, *vr); err != nil {
			return fmt.Errorf("write verify: %w", err)
		}
	}
	nem.persist(p.Dir)
	exportMetrics(f, resolved, summary, snaps)

	finalize(e, resolved.Manifest, summary, report, p, vr)
	return nil
}

// overrides routes explicitly-set flags into the highest-precedence config layer
// so they win over env vars and target defaults.
func overrides(cmd *cobra.Command, f *runFlags) map[string]any {
	o := map[string]any{}
	if cmd.Flags().Changed("connections") {
		o["connections"] = f.connections
	}
	if cmd.Flags().Changed("duration") {
		o["duration"] = f.duration
	}
	if cmd.Flags().Changed("warmup") {
		o["warmup"] = f.warmup
	}
	if cmd.Flags().Changed("consistency") {
		o["consistency"] = f.consistency
	}
	if cmd.Flags().Changed("endpoints") {
		o["endpoints"] = f.endpoints
	}
	if f.rate > 0 {
		o["ops_per_sec"] = f.rate
	}
	return o
}

// ensureEndpoints provisions a cluster when --provision is set, otherwise
// requires that endpoints were supplied. It returns a teardown func (a no-op
// unless a cluster was provisioned and --keep was not set) that the caller must
// defer so provisioned clusters are not leaked.
func ensureEndpoints(ctx context.Context, cmd *cobra.Command, f *runFlags, target string, r *config.Resolved) (func(), error) {
	teardown := func() {}
	if f.provision != "" {
		p, err := provision.Get(domain.Backend(f.provision))
		if err != nil {
			return teardown, err
		}
		// A target may ship a full compose bundle (e.g. ClickHouse + Keeper);
		// prefer it over a generically-rendered topology.
		if f.provision == string(domain.BackendCompose) {
			if b := filepath.Join("targets", target, infraDirName); fileExists(filepath.Join(b, "docker-compose.yml")) {
				r.Topology.Bundle = b
			}
		}
		cluster, err := p.Up(ctx, r.Topology)
		if err != nil {
			return teardown, fmt.Errorf("provision %s: %w", f.provision, err)
		}
		r.Driver.Cluster = cluster
		r.Driver.Endpoints = r.Driver.Endpoints[:0]
		for _, n := range cluster.Nodes {
			if n.Client != "" { // skip coordination-only nodes (e.g. keepers)
				r.Driver.Endpoints = append(r.Driver.Endpoints, n.Client)
			}
		}
		fmt.Fprintf(cmd.OutOrStdout(), "provisioned %d node(s) via %s (%d load endpoints)\n",
			len(cluster.Nodes), f.provision, len(r.Driver.Endpoints))
		if !f.keep {
			topo := r.Topology
			teardown = func() {
				fmt.Fprintln(cmd.OutOrStdout(), "tearing down provisioned cluster...")
				_ = p.Down(context.Background(), topo)
			}
		} else {
			fmt.Fprintln(cmd.OutOrStdout(), "  (--keep: leaving the cluster running; tear down with docker compose down -v)")
		}
	}
	if len(r.Driver.Endpoints) == 0 {
		return teardown, fmt.Errorf("no endpoints: pass --endpoints host:port or --provision compose")
	}
	return teardown, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// drive builds the engine components and executes the load loop, returning the
// runner report, the metrics recorder, the per-second time series, and the start time.
// driveParams fully specifies one load-execution unit, so it serves both the
// single-node run and each distributed worker (which shards the keyspace via
// workerID/workerCount and aligns its measure phase to startAt).
type driveParams struct {
	workload    domain.Workload
	cfg         driver.Config
	rate        domain.RateSpec
	connections int
	warmup      time.Duration
	duration    time.Duration
	seed        int64
	workerID    int
	workerCount int
	startAt     time.Time // zero => start now; else the synchronized measure start-gun
	observer    func(shard int) runner.OpObserver
	nemesis     *nemesisRun     // nil => no faults
	testType    domain.TestType // selects the arrival schedule
}

func drive(ctx context.Context, e Engine, pp driveParams) (runner.RunReport, *metrics.Recorder, []domain.LatencySnapshot, time.Time, error) {
	rec := metrics.New(metrics.Options{})
	count := pp.workerCount
	if count < 1 {
		count = 1
	}
	factory, err := workload.New(pp.workload, pp.seed, pp.workerID, count)
	if err != nil {
		return runner.RunReport{}, nil, nil, time.Time{}, fmt.Errorf("build workload: %w", err)
	}

	if !pp.startAt.IsZero() {
		if _, err := schedule.WaitUntil(ctx, pp.startAt); err != nil {
			return runner.RunReport{}, nil, nil, time.Time{}, err
		}
	}
	start := time.Now()
	sched := buildScheduler(start, pp.testType, pp.rate, pp.duration)
	pp.nemesis.begin(start.Add(pp.warmup)) // faults target the measure phase

	stopTicker := collectIntervals(rec)
	report, err := runner.Run(ctx, runner.Deps{
		Driver:   e.Driver,
		Cfg:      pp.cfg,
		Workload: pp.workload,
		Sched:    sched,
		Gen:      func(shard int) runner.OpGen { return factory.Generator(shard) },
		Sink:     func(shard int) runner.Sink { return rec.Local() },
		Observer: pp.observer,
	}, runner.Options{
		Connections: pp.connections,
		Warmup:      pp.warmup,
		Duration:    pp.duration,
	})
	out := stopTicker()
	if err != nil {
		return runner.RunReport{}, nil, nil, start, fmt.Errorf("run: %w", err)
	}
	return report, rec, out, start, nil
}

// collectIntervals samples the recorder once per second into a time series until
// the returned stop function is called; stop returns the collected snapshots.
func collectIntervals(rec *metrics.Recorder) func() []domain.LatencySnapshot {
	var (
		mu    sync.Mutex
		snaps []domain.LatencySnapshot
		done  = make(chan struct{})
		fin   = make(chan struct{})
	)
	go func() {
		defer close(fin)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				for _, s := range rec.TakeInterval(now, time.Second) {
					if s.Pct.Count > 0 {
						mu.Lock()
						snaps = append(snaps, s)
						mu.Unlock()
					}
				}
			}
		}
	}()
	stop := func() []domain.LatencySnapshot {
		close(done)
		<-fin
		mu.Lock()
		defer mu.Unlock()
		return snaps
	}
	return stop
}

func clientInfo() domain.ClientInfo {
	host, _ := os.Hostname()
	return domain.ClientInfo{Hostname: host, OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU()}
}
