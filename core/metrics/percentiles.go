// Package metrics is an HdrHistogram-backed latency recorder for proofload
// load runs. It is built for two competing demands: a lock-light hot path so
// many load goroutines can record concurrently, and lossless aggregation so a
// run's tail latency (p99.9) stays accurate across merges and distributed
// shipping.
//
// Latencies are stored internally in NANOSECONDS. This matches the
// HdrHistogram interval-log (.hlog) convention (the library scales the max
// value by the msec:nsec ratio when writing logs) and keeps sub-millisecond
// fidelity. All exported percentile values are converted to MILLISECONDS to
// satisfy domain.Percentiles.
//
// Percentiles are never averaged across sources; they are always recomputed
// from merged histograms.
package metrics

import (
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

const (
	// defaultSigFigs is the HdrHistogram precision when Options leaves it unset.
	defaultSigFigs = 3
	// defaultMaxLatency bounds the tracked range when Options leaves it unset.
	defaultMaxLatency = 60 * time.Second
	// lowestDiscernible is the smallest distinguishable latency (1 ns).
	lowestDiscernible = int64(1)
	// nsPerMs converts nanosecond histogram values to milliseconds.
	nsPerMs = 1e6
)

// newHist builds a histogram over [lowestDiscernible, high] nanoseconds.
func newHist(high int64, sig int) *hdrhistogram.Histogram {
	return hdrhistogram.New(lowestDiscernible, high, sig)
}

// msAt returns the value at percentile p (0..100) in milliseconds.
func msAt(h *hdrhistogram.Histogram, p float64) float64 {
	return float64(h.ValueAtQuantile(p)) / nsPerMs
}

// toPercentiles recomputes a millisecond latency summary from a histogram.
// It must be called on a merged histogram, never on averaged summaries.
func toPercentiles(h *hdrhistogram.Histogram) domain.Percentiles {
	return domain.Percentiles{
		Count: h.TotalCount(),
		Mean:  h.Mean() / nsPerMs,
		P50:   msAt(h, 50),
		P90:   msAt(h, 90),
		P95:   msAt(h, 95),
		P99:   msAt(h, 99),
		P999:  msAt(h, 99.9),
		P9999: msAt(h, 99.99),
		Max:   float64(h.Max()) / nsPerMs,
	}
}
