package report

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/proofload/proofload/core/domain"
)

// labeledValue is one row of the latency lollipop chart.
type labeledValue struct {
	Label string
	Value float64
}

// lollipopSVG renders a horizontal, log-scaled lollipop chart of latency
// percentiles as pure inline SVG. A log x-axis keeps the long tail (max, p99.9)
// visible next to the median. Returns "" when there is no positive data.
func lollipopSVG(vals []labeledValue) string {
	const w, rowH, top, left, right = 680.0, 30.0, 26.0, 70.0, 90.0
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, v := range vals {
		if v.Value > 0 {
			lo, hi = math.Min(lo, v.Value), math.Max(hi, v.Value)
		}
	}
	if math.IsInf(lo, 1) {
		return ""
	}
	if lo == hi {
		lo, hi = lo/10, hi*10
	}
	llo, lhi := math.Log10(lo), math.Log10(hi)
	plotW := w - left - right
	x := func(v float64) float64 {
		if v <= 0 {
			return left
		}
		return left + (math.Log10(v)-llo)/(lhi-llo)*plotW
	}
	h := top + float64(len(vals))*rowH + 12
	var b strings.Builder
	b.WriteString(sprintf(`<svg viewBox="0 0 %s %s" role="img" aria-label="latency percentiles">`, coord(w), coord(h)))
	// Power-of-ten gridlines with tick labels.
	for p := math.Ceil(llo); p <= math.Floor(lhi); p++ {
		gx := x(math.Pow(10, p))
		b.WriteString(sprintf(`<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="var(--grid)" stroke-width="1"/>`,
			coord(gx), coord(top-6), coord(gx), coord(h-16)))
		b.WriteString(sprintf(`<text x="%s" y="%s" fill="var(--muted)" font-size="10" text-anchor="middle">%s</text>`,
			coord(gx), coord(h-4), esc(fmtFloat(math.Pow(10, p)))))
	}
	for i, v := range vals {
		y := top + float64(i)*rowH + rowH/2
		px := x(v.Value)
		b.WriteString(sprintf(`<text x="%s" y="%s" fill="var(--ink2)" font-size="12" text-anchor="end" dominant-baseline="middle">%s</text>`,
			coord(left-10), coord(y), esc(v.Label)))
		b.WriteString(sprintf(`<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="var(--series-1)" stroke-width="2"/>`,
			coord(left), coord(y), coord(px), coord(y)))
		b.WriteString(sprintf(`<circle cx="%s" cy="%s" r="4.5" fill="var(--series-1)"/>`, coord(px), coord(y)))
		b.WriteString(sprintf(`<text x="%s" y="%s" fill="var(--ink)" font-size="12" text-anchor="start" dominant-baseline="middle">%s</text>`,
			coord(px+9), coord(y), esc(fmtMs(v.Value))))
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// xy is a single point on a time-series line (X in seconds since start).
type xy struct{ X, Y float64 }

// lineSeries is one named line with a CSS color role.
type lineSeries struct {
	Name  string
	Color string // e.g. "var(--series-1)"
	Pts   []xy
}

// lineChartSVG renders one or more lines over time as pure inline SVG, with a
// zero-based linear y-axis, hairline gridlines, and time ticks. Returns "" when
// there is nothing to plot.
func lineChartSVG(unit string, series []lineSeries) string {
	const w, h, left, right, top, bot = 680.0, 250.0, 60.0, 16.0, 16.0, 30.0
	var xmax, ymax float64
	n := 0
	for _, s := range series {
		for _, p := range s.Pts {
			xmax, ymax = math.Max(xmax, p.X), math.Max(ymax, p.Y)
			n++
		}
	}
	if n == 0 {
		return ""
	}
	if ymax == 0 {
		ymax = 1
	}
	ymax = niceMax(ymax)
	if xmax == 0 {
		xmax = 1
	}
	plotW, plotH := w-left-right, h-top-bot
	px := func(v float64) float64 { return left + v/xmax*plotW }
	py := func(v float64) float64 { return top + plotH - v/ymax*plotH }

	var b strings.Builder
	b.WriteString(sprintf(`<svg viewBox="0 0 %s %s" role="img" aria-label="time series (%s)">`, coord(w), coord(h), esc(unit)))
	// Y gridlines + labels (0..ymax in 4 steps).
	for i := 0; i <= 4; i++ {
		yv := ymax * float64(i) / 4
		gy := py(yv)
		b.WriteString(sprintf(`<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="var(--grid)" stroke-width="1"/>`,
			coord(left), coord(gy), coord(w-right), coord(gy)))
		b.WriteString(sprintf(`<text x="%s" y="%s" fill="var(--muted)" font-size="10" text-anchor="end" dominant-baseline="middle">%s</text>`,
			coord(left-8), coord(gy), esc(fmtFloat(yv))))
	}
	// X ticks (0..xmax in seconds, 4 steps).
	for i := 0; i <= 4; i++ {
		xv := xmax * float64(i) / 4
		gx := px(xv)
		b.WriteString(sprintf(`<text x="%s" y="%s" fill="var(--muted)" font-size="10" text-anchor="middle">%ss</text>`,
			coord(gx), coord(h-8), esc(fmtFloat(xv))))
	}
	b.WriteString(sprintf(`<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="var(--axis)" stroke-width="1"/>`,
		coord(left), coord(top+plotH), coord(w-right), coord(top+plotH)))
	for _, s := range series {
		if len(s.Pts) == 0 {
			continue
		}
		var pts strings.Builder
		for _, p := range s.Pts {
			pts.WriteString(sprintf("%s,%s ", coord(px(p.X)), coord(py(p.Y))))
		}
		b.WriteString(sprintf(`<polyline fill="none" stroke="%s" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" points="%s"/>`,
			s.Color, strings.TrimSpace(pts.String())))
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// niceMax rounds a maximum up to a readable axis bound (1/2/5 * 10^k).
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	exp := math.Floor(math.Log10(v))
	base := math.Pow(10, exp)
	switch f := v / base; {
	case f <= 1:
		return base
	case f <= 2:
		return 2 * base
	case f <= 5:
		return 5 * base
	default:
		return 10 * base
	}
}

// tPoint is one aggregated interval of the run time-series.
type tPoint struct {
	T              time.Time
	Throughput     float64
	P50, P99, P999 float64
}

// aggregate collapses per-op snapshots into one series over time: throughput is
// summed across ops in each interval, while the latency percentiles are taken
// from the busiest op (largest sample count) so the three latency lines stay
// mutually consistent. Points are returned sorted by time.
func aggregate(snaps []domain.LatencySnapshot) []tPoint {
	type acc struct {
		tp        tPoint
		bestCount int64
	}
	byT := map[int64]*acc{}
	for _, s := range snaps {
		k := s.T.UnixMicro()
		a := byT[k]
		if a == nil {
			a = &acc{tp: tPoint{T: s.T}, bestCount: -1}
			byT[k] = a
		}
		a.tp.Throughput += s.Throughput
		if s.Pct.Count > a.bestCount {
			a.bestCount = s.Pct.Count
			a.tp.P50, a.tp.P99, a.tp.P999 = s.Pct.P50, s.Pct.P99, s.Pct.P999
		}
	}
	out := make([]tPoint, 0, len(byT))
	for _, a := range byT {
		out = append(out, a.tp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T.Before(out[j].T) })
	return out
}
