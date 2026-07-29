package verify

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// Options configures a Reconcile pass.
type Options struct {
	// ReadOp is the driver's read operation type (e.g. "read", "r").
	ReadOp domain.OpType
	// Workload is used to Prepare the read-back connection so drivers that rely
	// on prepared statements (e.g. PostgreSQL) can serve the read op. The zero
	// value is fine for drivers that need no preparation.
	Workload domain.Workload
	// ConvergenceSample is the number of keys to check across all nodes.
	ConvergenceSample int
	// ConvergenceTimeout bounds how long replicas are polled per key until they
	// agree. Zero disables polling (a single snapshot check).
	ConvergenceTimeout time.Duration
}

// convergePoll is the delay between replica reads while waiting to converge.
const convergePoll = 5 * time.Millisecond

// Reconcile reads every expected key back from the target and compares
// checksums, then (for multi-node ClusterAware targets) samples keys for
// replica convergence. It never mutates the target.
func Reconcile(ctx context.Context, d driver.Driver, cfg driver.Config, log *Log, o Options) (domain.VerifyReport, error) {
	report := domain.VerifyReport{Model: domain.VerifyReconciliation}
	expected := log.Expected()
	report.Checked = int64(len(expected))

	conn, err := d.Connect(ctx, cfg)
	if err != nil {
		return report, fmt.Errorf("verify: connect: %w", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, o.Workload); err != nil {
		return report, fmt.Errorf("verify: prepare read-back connection: %w", err)
	}

	readBack(ctx, conn, expected, o, &report)
	if ca, ok := d.(driver.ClusterAware); ok {
		checkConvergence(ctx, ca, cfg, expected, o, &report)
	}

	report.Verdict = domain.VerdictPass
	if report.Lost > 0 || len(report.Anomalies) > 0 {
		report.Verdict = domain.VerdictFail
	}
	return report, nil
}

// readBack reads each expected key and records loss (absent) or corruption
// (checksum mismatch). Duplication and ordering are not applicable here.
func readBack(ctx context.Context, conn driver.Conn, expected map[int64]WriteRecord, o Options, report *domain.VerifyReport) {
	for key, want := range expected {
		res := conn.Execute(ctx, domain.Operation{Type: o.ReadOp, Key: key})
		b, present := coerceBytes(res.Observed)
		if res.Err != nil || !present {
			report.Lost++
			report.Anomalies = append(report.Anomalies, domain.Anomaly{
				Kind:    "data-loss",
				Detail:  fmt.Sprintf("key %d absent on read-back", key),
				Witness: []string{witness(key)},
			})
			continue
		}
		if got := Checksum(b); got != want.Checksum {
			report.Anomalies = append(report.Anomalies, domain.Anomaly{
				Kind:    "corruption",
				Detail:  fmt.Sprintf("key %d checksum %d, expected %d", key, got, want.Checksum),
				Witness: []string{witness(key)},
			})
		}
	}
}

// checkConvergence samples keys and confirms every replica agrees, polling up
// to o.ConvergenceTimeout per key. It records the worst per-key convergence
// time as MaxStaleness and the total phase time as ConvergedIn.
func checkConvergence(ctx context.Context, ca driver.ClusterAware, cfg driver.Config, expected map[int64]WriteRecord, o Options, report *domain.VerifyReport) {
	nodes := ca.Nodes(cfg)
	if len(nodes) <= 1 || o.ConvergenceSample <= 0 {
		return
	}
	start := time.Now()
	var maxStale time.Duration
	for _, key := range sampleKeys(expected, o.ConvergenceSample) {
		converged, dur, err := convergeKey(ctx, ca, nodes, key, o.ConvergenceTimeout)
		if dur > maxStale {
			maxStale = dur
		}
		if err != nil || !converged {
			report.Anomalies = append(report.Anomalies, domain.Anomaly{
				Kind:    "divergence",
				Detail:  fmt.Sprintf("key %d replicas did not converge within %s", key, o.ConvergenceTimeout),
				Witness: []string{witness(key)},
			})
		}
	}
	report.MaxStaleness = maxStale
	report.ConvergedIn = time.Since(start)
}

// convergeKey polls every replica for key until they agree or the timeout
// elapses. It returns whether they converged and how long that took.
func convergeKey(ctx context.Context, ca driver.ClusterAware, nodes []domain.Node, key int64, timeout time.Duration) (bool, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		agree, err := nodesAgree(ctx, ca, nodes, key)
		if err != nil {
			return false, time.Since(start), err
		}
		if agree {
			return true, time.Since(start), nil
		}
		if timeout <= 0 || !time.Now().Before(deadline) {
			return false, time.Since(start), nil
		}
		select {
		case <-ctx.Done():
			return false, time.Since(start), ctx.Err()
		case <-time.After(convergePoll):
		}
	}
}

// nodesAgree reports whether every node returns the same value for key.
func nodesAgree(ctx context.Context, ca driver.ClusterAware, nodes []domain.Node, key int64) (bool, error) {
	var first uint64
	for i, n := range nodes {
		res, err := ca.ReadKeyFrom(ctx, n, key)
		if err != nil {
			return false, err
		}
		b, _ := coerceBytes(res.Observed)
		sum := Checksum(b)
		if i == 0 {
			first = sum
			continue
		}
		if sum != first {
			return false, nil
		}
	}
	return true, nil
}

// sampleKeys returns up to n keys from expected (map order is unspecified,
// which is fine for a representative sample).
func sampleKeys(expected map[int64]WriteRecord, n int) []int64 {
	out := make([]int64, 0, n)
	for k := range expected {
		if len(out) >= n {
			break
		}
		out = append(out, k)
	}
	return out
}

// coerceBytes normalizes a driver's OpResult.Observed into bytes. present is
// false when the key is absent (nil interface or a typed-nil []byte).
func coerceBytes(v any) (b []byte, present bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case []byte:
		return x, x != nil
	case string:
		return []byte(x), true
	default:
		return nil, false
	}
}

func witness(key int64) string { return strconv.FormatInt(key, 10) }
