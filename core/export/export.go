// Package export implements the optional time-series-database exporter for
// proofload. It pushes per-second latency snapshots to a TSDB over HTTP using
// text wire formats (no protobuf, no client libraries), so a running benchmark
// can feed a live Grafana dashboard.
//
// The local-first default storage (DuckDB/HTML) lives elsewhere; this package is
// the optional "stream it somewhere central" half of the storage story. Two wire
// formats are supported:
//
//   - InfluxDB / VictoriaMetrics line protocol, via NewInflux. One line per
//     snapshot, batched into a single POST. Per-point nanosecond timestamps are
//     preserved, so the full time-series is retained.
//   - Prometheus Pushgateway text exposition format, via NewPushgateway. The
//     exposition format has no per-point timestamp and Pushgateway keeps only the
//     most recent push, so this exporter emits a run summary (the latest snapshot
//     per operation) rather than the full series. See NewPushgateway.
package export

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// Exporter pushes latency snapshots to a time-series database. Implementations
// are safe to reuse across intervals; call Close once when the run is done.
type Exporter interface {
	// Export pushes the given snapshots, tagging every series with labels (plus
	// the per-snapshot operation type). It returns an error if the transport
	// fails or the server answers with a non-2xx status.
	Export(ctx context.Context, snaps []domain.LatencySnapshot, labels map[string]string) error
	// Close releases transport resources. It is idempotent.
	Close() error
}

// New builds an Exporter for the given kind ("influx" or "pushgateway") pointing
// at url. It is the entry point for the runner's --export <kind>=<url> flag.
//
// For "influx", url is the write endpoint (e.g. http://host:8086/write?db=proofload
// for InfluxDB, or http://host:8428/write for VictoriaMetrics).
//
// For "pushgateway", url is the Pushgateway base URL (e.g. http://host:9091); the
// job defaults to defaultJob. Use NewPushgateway directly to set a custom job.
func New(kind, url string) (Exporter, error) {
	switch kind {
	case "influx":
		return NewInflux(url), nil
	case "pushgateway":
		return NewPushgateway(url, defaultJob), nil
	default:
		return nil, fmt.Errorf("export: unknown kind %q (want \"influx\" or \"pushgateway\")", kind)
	}
}

// defaultJob is the Pushgateway job used when New is called without an explicit one.
const defaultJob = "proofload"

// newHTTPClient returns the shared HTTP client configuration for exporters.
func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

// postBody POSTs body to url and treats any non-2xx status as an error.
func postBody(ctx context.Context, client *http.Client, url, contentType string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("export: build request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("export: post to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("export: %s returned %s", url, resp.Status)
	}
	return nil
}
