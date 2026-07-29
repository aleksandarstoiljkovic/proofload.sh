package export

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// capture is a test server that records the last request path, content type and body.
type capture struct {
	server      *httptest.Server
	path        string
	contentType string
	body        string
	status      int
}

func newCapture() *capture {
	c := &capture{status: http.StatusNoContent}
	c.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.path = r.URL.Path
		c.contentType = r.Header.Get("Content-Type")
		c.body = string(b)
		w.WriteHeader(c.status)
	}))
	return c
}

func (c *capture) close() { c.server.Close() }

// sampleSnaps returns two snapshots for the same op at consecutive seconds. The
// "workload" label deliberately contains a comma, space and equals sign to
// exercise escaping.
func sampleSnaps() []domain.LatencySnapshot {
	t0 := time.Unix(1700000000, 0).UTC()
	mk := func(sec int, p50, thr float64, errs int64) domain.LatencySnapshot {
		return domain.LatencySnapshot{
			T:          t0.Add(time.Duration(sec) * time.Second),
			OpType:     "read",
			Throughput: thr,
			InFlight:   4,
			Errors:     errs,
			Pct: domain.Percentiles{
				Count: 100, Mean: 0.3, P50: p50, P90: 0.5,
				P95: 0.7, P99: 3.6, P999: 9.9, P9999: 12.5, Max: 20,
			},
		}
	}
	return []domain.LatencySnapshot{mk(0, 0.26, 1000, 1), mk(1, 0.28, 1100, 2)}
}

func labelsWithSpecials() map[string]string {
	return map[string]string{"target": "pg", "workload": "oltp a,b=c"}
}

func TestInfluxExport(t *testing.T) {
	cap := newCapture()
	defer cap.close()

	exp := NewInflux(cap.server.URL + "/write")
	defer exp.Close()

	if err := exp.Export(context.Background(), sampleSnaps(), labelsWithSpecials()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	lines := strings.Split(strings.TrimRight(cap.body, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines (one per snapshot), got %d: %q", len(lines), cap.body)
	}

	first := lines[0]
	wants := []string{
		"proofload_latency,op=read,target=pg,workload=oltp", // measurement + sorted tags
		`workload=oltp\ a\,b\=c`,                            // escaped space, comma, equals
		"p50=0.26", "p99=3.6", "throughput=1000",            // float fields
		"errors=1i", "in_flight=4i", "count=100i", // integer fields (i suffix)
	}
	for _, w := range wants {
		if !strings.Contains(first, w) {
			t.Errorf("line %q missing %q", first, w)
		}
	}

	// Timestamp is the snapshot's Unix nanoseconds (integer) as the last token.
	fields := strings.Fields(first)
	ts := fields[len(fields)-1]
	if ts != "1700000000000000000" {
		t.Errorf("want integer-nanosecond timestamp 1700000000000000000, got %q", ts)
	}
	if !strings.HasPrefix(cap.contentType, "text/plain") {
		t.Errorf("want text/plain content type, got %q", cap.contentType)
	}
}

func TestPushgatewayExport(t *testing.T) {
	cap := newCapture()
	defer cap.close()

	exp := NewPushgateway(cap.server.URL, "bench")
	defer exp.Close()

	if err := exp.Export(context.Background(), sampleSnaps(), labelsWithSpecials()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if cap.path != "/metrics/job/bench" {
		t.Errorf("want path /metrics/job/bench, got %q", cap.path)
	}

	wantSubstrings := []string{
		"# TYPE proofload_latency_ms gauge",
		`proofload_latency_ms{op="read",quantile="0.5",target="pg",workload="oltp a,b=c"} 0.28`,
		`quantile="0.99"`,
		"# TYPE proofload_throughput gauge",
		"# TYPE proofload_errors gauge",
	}
	for _, w := range wantSubstrings {
		if !strings.Contains(cap.body, w) {
			t.Errorf("body missing %q\nbody:\n%s", w, cap.body)
		}
	}

	// Only the latest snapshot per op is pushed (run summary), so exactly one
	// quantile="0.5" line should appear despite two input snapshots.
	if n := strings.Count(cap.body, `quantile="0.5"`); n != 1 {
		t.Errorf("want exactly 1 latest-per-op quantile line, got %d", n)
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		kind    string
		wantErr bool
		typ     string
	}{
		{"influx", false, "*export.influxExporter"},
		{"pushgateway", false, "*export.pushgwExporter"},
		{"unknown", true, ""},
		{"", true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.kind, func(t *testing.T) {
			exp, err := New(tc.kind, "http://localhost:0/write")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error for kind %q, got nil", tc.kind)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var got string
			switch exp.(type) {
			case *influxExporter:
				got = "*export.influxExporter"
			case *pushgwExporter:
				got = "*export.pushgwExporter"
			}
			if got != tc.typ {
				t.Errorf("want type %s, got %s", tc.typ, got)
			}
		})
	}
}

func TestExportServerErrorSurfaces(t *testing.T) {
	cap := newCapture()
	cap.status = http.StatusInternalServerError
	defer cap.close()

	exporters := map[string]Exporter{
		"influx":      NewInflux(cap.server.URL + "/write"),
		"pushgateway": NewPushgateway(cap.server.URL, "bench"),
	}
	for name, exp := range exporters {
		t.Run(name, func(t *testing.T) {
			err := exp.Export(context.Background(), sampleSnaps(), nil)
			if err == nil {
				t.Fatalf("want error on non-2xx response, got nil")
			}
			if !strings.Contains(err.Error(), "500") {
				t.Errorf("error should mention the status: %v", err)
			}
		})
	}
}

func TestExportEmptyIsNoop(t *testing.T) {
	cap := newCapture()
	cap.status = http.StatusInternalServerError // would fail if a request were sent
	defer cap.close()

	exp := NewInflux(cap.server.URL + "/write")
	if err := exp.Export(context.Background(), nil, nil); err != nil {
		t.Fatalf("empty export should be a no-op, got %v", err)
	}
}
