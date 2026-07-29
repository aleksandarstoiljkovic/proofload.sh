package export

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// pushgwExporter writes Prometheus text exposition format to a Pushgateway.
type pushgwExporter struct {
	pushURL string
	client  *http.Client
}

// NewPushgateway returns an Exporter that POSTs Prometheus text to
// <baseURL>/metrics/job/<job>.
//
// The exposition format carries no per-point timestamp, and a Pushgateway keeps
// only the most recent push for a job. This exporter therefore emits a run
// summary: the latest snapshot for each operation type (last write wins within a
// call), not the full per-second series. Use NewInflux when you need the whole
// time-series with timestamps.
func NewPushgateway(baseURL, job string) Exporter {
	url := strings.TrimRight(baseURL, "/") + "/metrics/job/" + job
	return &pushgwExporter{pushURL: url, client: newHTTPClient()}
}

// gauge names emitted by the Pushgateway exporter.
const (
	metricLatency    = "proofload_latency_ms"
	metricLatencyMax = "proofload_latency_max_ms"
	metricThroughput = "proofload_throughput"
	metricInFlight   = "proofload_in_flight"
	metricErrors     = "proofload_errors"
)

// quantile pairs a Prometheus quantile label with its percentile accessor.
type quantile struct {
	label string
	value func(domain.Percentiles) float64
}

var quantiles = []quantile{
	{"0.5", func(p domain.Percentiles) float64 { return p.P50 }},
	{"0.9", func(p domain.Percentiles) float64 { return p.P90 }},
	{"0.95", func(p domain.Percentiles) float64 { return p.P95 }},
	{"0.99", func(p domain.Percentiles) float64 { return p.P99 }},
	{"0.999", func(p domain.Percentiles) float64 { return p.P999 }},
	{"0.9999", func(p domain.Percentiles) float64 { return p.P9999 }},
}

// Export builds the exposition body for the latest snapshot per op and posts it.
func (e *pushgwExporter) Export(ctx context.Context, snaps []domain.LatencySnapshot, labels map[string]string) error {
	if len(snaps) == 0 {
		return nil
	}
	body := buildExposition(latestPerOp(snaps), labels)
	return postBody(ctx, e.client, e.pushURL, "text/plain; version=0.0.4", []byte(body))
}

// Close releases idle connections; the exporter may not be reused afterward.
func (e *pushgwExporter) Close() error {
	e.client.CloseIdleConnections()
	return nil
}

// latestPerOp keeps the last snapshot seen for each operation type, ordered by op
// name, so the body has no duplicate label sets (which Pushgateway would reject).
func latestPerOp(snaps []domain.LatencySnapshot) []domain.LatencySnapshot {
	byOp := make(map[domain.OpType]domain.LatencySnapshot, len(snaps))
	for _, s := range snaps {
		byOp[s.OpType] = s
	}
	ops := make([]string, 0, len(byOp))
	for op := range byOp {
		ops = append(ops, string(op))
	}
	sort.Strings(ops)

	out := make([]domain.LatencySnapshot, 0, len(ops))
	for _, op := range ops {
		out = append(out, byOp[domain.OpType(op)])
	}
	return out
}

// buildExposition renders all gauge families, each preceded by its TYPE line.
func buildExposition(snaps []domain.LatencySnapshot, labels map[string]string) string {
	var b strings.Builder
	writeQuantileFamily(&b, snaps, labels)
	writeScalarFamily(&b, metricLatencyMax, snaps, labels, func(s domain.LatencySnapshot) string { return f(s.Pct.Max) })
	writeScalarFamily(&b, metricThroughput, snaps, labels, func(s domain.LatencySnapshot) string { return f(s.Throughput) })
	writeScalarFamily(&b, metricInFlight, snaps, labels, func(s domain.LatencySnapshot) string { return strconv.Itoa(s.InFlight) })
	writeScalarFamily(&b, metricErrors, snaps, labels, func(s domain.LatencySnapshot) string { return strconv.FormatInt(s.Errors, 10) })
	return b.String()
}

// writeQuantileFamily emits proofload_latency_ms with a quantile label per point.
func writeQuantileFamily(b *strings.Builder, snaps []domain.LatencySnapshot, labels map[string]string) {
	b.WriteString("# TYPE " + metricLatency + " gauge\n")
	for _, s := range snaps {
		for _, q := range quantiles {
			b.WriteString(metricLatency)
			b.WriteString(promLabels(string(s.OpType), labels, q.label))
			b.WriteByte(' ')
			b.WriteString(f(q.value(s.Pct)))
			b.WriteByte('\n')
		}
	}
}

// writeScalarFamily emits one gauge line per snapshot for a single-value metric.
func writeScalarFamily(b *strings.Builder, name string, snaps []domain.LatencySnapshot, labels map[string]string, val func(domain.LatencySnapshot) string) {
	b.WriteString("# TYPE " + name + " gauge\n")
	for _, s := range snaps {
		b.WriteString(name)
		b.WriteString(promLabels(string(s.OpType), labels, ""))
		b.WriteByte(' ')
		b.WriteString(val(s))
		b.WriteByte('\n')
	}
}

// promLabels renders a sorted, escaped Prometheus label set. The op label is
// always present; a non-empty quantile adds a "quantile" label.
func promLabels(op string, labels map[string]string, quantile string) string {
	set := map[string]string{"op": op}
	for k, v := range labels {
		set[k] = v
	}
	if quantile != "" {
		set["quantile"] = quantile
	}
	var b strings.Builder
	b.WriteByte('{')
	for idx, k := range sortedKeys(set) {
		if idx > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapePromLabelValue(set[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escapePromLabelValue escapes backslashes, double quotes and newlines per the
// Prometheus text exposition rules.
func escapePromLabelValue(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}
