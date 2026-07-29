package export

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// measurement is the InfluxDB measurement name for every exported snapshot.
const measurement = "proofload_latency"

// influxExporter writes InfluxDB / VictoriaMetrics line protocol to a write URL.
type influxExporter struct {
	writeURL string
	client   *http.Client
}

// NewInflux returns an Exporter that batches all snapshots of a call into one
// POST of InfluxDB line protocol to writeURL. VictoriaMetrics accepts the same
// format on its /write endpoint. Latency fields are milliseconds; timestamps are
// the snapshot's own Unix nanoseconds, so the full per-second series is kept.
func NewInflux(writeURL string) Exporter {
	return &influxExporter{writeURL: writeURL, client: newHTTPClient()}
}

// Export encodes one line per snapshot and posts them as a single body.
func (e *influxExporter) Export(ctx context.Context, snaps []domain.LatencySnapshot, labels map[string]string) error {
	if len(snaps) == 0 {
		return nil
	}
	var b strings.Builder
	for _, s := range snaps {
		writeInfluxLine(&b, s, labels)
	}
	return postBody(ctx, e.client, e.writeURL, "text/plain; charset=utf-8", []byte(b.String()))
}

// Close releases idle connections; the exporter may not be reused afterward.
func (e *influxExporter) Close() error {
	e.client.CloseIdleConnections()
	return nil
}

// writeInfluxLine appends one line-protocol record (with trailing newline) for a
// single snapshot: measurement,<sorted tags> <fields> <unix_nanos>.
func writeInfluxLine(b *strings.Builder, s domain.LatencySnapshot, labels map[string]string) {
	b.WriteString(measurement)
	writeInfluxTags(b, string(s.OpType), labels)
	b.WriteByte(' ')
	writeInfluxFields(b, s)
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(s.T.UnixNano(), 10))
	b.WriteByte('\n')
}

// writeInfluxTags appends the comma-separated, sorted tag set. The "op" tag is
// merged with the caller's labels; equal keys resolve to the caller's label.
func writeInfluxTags(b *strings.Builder, op string, labels map[string]string) {
	tags := map[string]string{"op": op}
	for k, v := range labels {
		tags[k] = v
	}
	for _, k := range sortedKeys(tags) {
		b.WriteByte(',')
		b.WriteString(escapeInfluxTag(k))
		b.WriteByte('=')
		b.WriteString(escapeInfluxTag(tags[k]))
	}
}

// writeInfluxFields appends the comma-separated field set. Percentiles and
// throughput are floats; count, in_flight and errors are integers (i suffix).
func writeInfluxFields(b *strings.Builder, s domain.LatencySnapshot) {
	p := s.Pct
	b.WriteString("p50=" + f(p.P50))
	b.WriteString(",p90=" + f(p.P90))
	b.WriteString(",p95=" + f(p.P95))
	b.WriteString(",p99=" + f(p.P99))
	b.WriteString(",p999=" + f(p.P999))
	b.WriteString(",p9999=" + f(p.P9999))
	b.WriteString(",mean=" + f(p.Mean))
	b.WriteString(",max=" + f(p.Max))
	b.WriteString(",throughput=" + f(s.Throughput))
	b.WriteString(",count=" + i(p.Count))
	b.WriteString(",in_flight=" + i(int64(s.InFlight)))
	b.WriteString(",errors=" + i(s.Errors))
}

// f formats a float field value compactly (no exponent padding, minimal digits).
func f(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// i formats an integer field value with the line-protocol integer suffix.
func i(v int64) string { return strconv.FormatInt(v, 10) + "i" }

// escapeInfluxTag escapes commas, equals signs and spaces in tag keys/values per
// the line-protocol rules.
func escapeInfluxTag(s string) string {
	r := strings.NewReplacer(",", `\,`, "=", `\=`, " ", `\ `)
	return r.Replace(s)
}

// sortedKeys returns the map keys in ascending order for deterministic output.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
