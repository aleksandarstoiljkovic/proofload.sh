package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

// WriteHLog writes the Recorder's merged histograms as an HdrHistogram interval
// log (.hlog) — the run's canonical latency file. This is the real HdrHistogram
// log format (readable by the Java HdrHistogram tools and hdrhistogram-go's own
// HistogramLogReader), not a JSON substitute. One tagged interval line is
// emitted for the overall distribution plus one per op type. Values are stored
// in nanoseconds; the writer scales the reported max by the msec:nsec ratio per
// the format convention. start stamps the log start time.
func WriteHLog(w io.Writer, r *Recorder, start time.Time) error {
	m := r.collect()
	lw := hdrhistogram.NewHistogramLogWriter(w)

	if err := lw.OutputLogFormatVersion(); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	startMs := start.UnixMilli()
	if err := lw.OutputStartTime(startMs); err != nil {
		return fmt.Errorf("write start time: %w", err)
	}
	if err := lw.OutputLegend(); err != nil {
		return fmt.Errorf("write legend: %w", err)
	}

	if err := writeInterval(lw, m.overall, "overall", startMs); err != nil {
		return err
	}

	ops := make([]domain.OpType, 0, len(m.byOp))
	for op := range m.byOp {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	for _, op := range ops {
		if err := writeInterval(lw, m.byOp[op], tagFor(op), startMs); err != nil {
			return err
		}
	}
	return nil
}

// writeInterval stamps and emits one tagged cumulative histogram as an interval
// line. The interval length is zero: the file ships a single cumulative
// histogram per tag rather than a per-second series (that series is the
// Parquet/TSDB output of TakeInterval).
func writeInterval(lw *hdrhistogram.HistogramLogWriter, h *hdrhistogram.Histogram, tag string, startMs int64) error {
	h.SetTag(tag)
	h.SetStartTimeMs(startMs)
	h.SetEndTimeMs(startMs)
	if err := lw.OutputIntervalHistogram(h); err != nil {
		return fmt.Errorf("write interval %q: %w", tag, err)
	}
	return nil
}

// tagFor sanitizes an op type into a valid .hlog tag: the format forbids
// commas, spaces, and line breaks in tags.
func tagFor(op domain.OpType) string {
	r := strings.NewReplacer(",", "_", " ", "_", "\t", "_", "\r", "_", "\n", "_")
	s := r.Replace(string(op))
	if s == "" {
		return "op"
	}
	return s
}
