package report

import (
	"sort"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// headerHTML renders the run title and identity pills.
func headerHTML(m domain.Manifest) string {
	var b strings.Builder
	b.WriteString(sprintf(`<h1>%s</h1>`, esc(m.Target)))
	b.WriteString(sprintf(`<p class="sub">Run report &middot; %s</p>`, esc(string(m.RunID))))
	pills := [][2]string{
		{"workload", m.Workload}, {"mode", string(m.Mode)},
		{"rate", rateLabel(m.Rate)}, {"consistency", m.Consistency},
	}
	for _, p := range pills {
		if p[1] == "" {
			continue
		}
		b.WriteString(sprintf(`<span class="pill">%s: %s</span>`, esc(p[0]), esc(p[1])))
	}
	return b.String()
}

func rateLabel(r domain.RateSpec) string {
	if r.Mode == domain.RateFixed {
		return sprintf("fixed %s ops/s", fmtInt(int64(r.OpsPerSec)))
	}
	return string(r.Mode)
}

// metadataHTML renders the reproducibility table from the manifest.
func metadataHTML(m domain.Manifest) string {
	rows := [][2]string{
		{"Target", m.Target}, {"Target version", m.TargetVersion},
		{"Workload", m.Workload}, {"Mode", string(m.Mode)},
		{"Rate", rateLabel(m.Rate)}, {"Duration", fmtDur(m.Duration)},
		{"Warmup", fmtDur(m.Warmup)}, {"Connections", fmtInt(int64(m.Connections))},
		{"Seed", fmtInt(m.Seed)}, {"Trials", fmtInt(int64(m.Trials))},
		{"Consistency", m.Consistency},
		{"Replication factor", fmtInt(int64(m.Cluster.ReplicationFactor))},
		{"Cluster nodes", fmtInt(int64(len(m.Cluster.Nodes)))},
		{"Engine version", m.EngineVersion}, {"Git SHA", m.GitSHA},
		{"Client", clientLabel(m.Client)}, {"Created at", fmtTime(m.CreatedAt)},
	}
	var b strings.Builder
	b.WriteString(`<h2>Run metadata</h2><div class="card"><div class="tblwrap"><table><tbody>`)
	for _, r := range rows {
		if r[1] == "" {
			continue
		}
		b.WriteString(sprintf(`<tr><th>%s</th><td>%s</td></tr>`, esc(r[0]), esc(r[1])))
	}
	for _, k := range sortedKeys(m.Labels) {
		b.WriteString(sprintf(`<tr><th>label: %s</th><td>%s</td></tr>`, esc(k), esc(m.Labels[k])))
	}
	b.WriteString(`</tbody></table></div></div>`)
	return b.String()
}

func clientLabel(c domain.ClientInfo) string {
	if c.Hostname == "" {
		return ""
	}
	return sprintf("%s (%s/%s, %s CPUs)", c.Hostname, c.OS, c.Arch, fmtInt(int64(c.CPUs)))
}

// summaryHTML renders headline stats and the latency percentile lollipop.
func summaryHTML(r domain.RunResult) string {
	var b strings.Builder
	b.WriteString(`<h2>Summary</h2><div class="card">`)
	b.WriteString(`<div class="stats">`)
	b.WriteString(statHTML("Throughput", fmtFloat(r.Throughput)+" ops/s"))
	b.WriteString(statHTML("Total ops", fmtInt(r.Total)))
	b.WriteString(statHTML("Errors", fmtInt(r.Errors)))
	b.WriteString(statHTML("Duration", fmtDur(r.Duration)))
	b.WriteString(`</div>`)
	vals := []labeledValue{
		{"p50", r.Overall.P50}, {"p90", r.Overall.P90}, {"p95", r.Overall.P95},
		{"p99", r.Overall.P99}, {"p99.9", r.Overall.P999}, {"max", r.Overall.Max},
	}
	if svg := lollipopSVG(vals); svg != "" {
		b.WriteString(`<p class="muted" style="margin:8px 0 2px;font-size:.85rem">Overall latency (log scale)</p>`)
		b.WriteString(svg)
	}
	b.WriteString(`</div>`)
	return b.String()
}

func statHTML(k, v string) string {
	return sprintf(`<div class="stat"><div class="k">%s</div><div class="v">%s</div></div>`, esc(k), esc(v))
}

// byOpHTML renders the per-operation percentile table.
func byOpHTML(r domain.RunResult) string {
	ops := make([]string, 0, len(r.ByOp))
	for op := range r.ByOp {
		ops = append(ops, string(op))
	}
	sort.Strings(ops)
	var b strings.Builder
	b.WriteString(`<h2>Per-operation latency</h2><div class="card"><div class="tblwrap"><table>`)
	b.WriteString(`<thead><tr><th>Op</th><th class="num">Count</th><th class="num">p50</th>` +
		`<th class="num">p90</th><th class="num">p95</th><th class="num">p99</th>` +
		`<th class="num">p99.9</th><th class="num">p99.99</th><th class="num">Max</th></tr></thead><tbody>`)
	for _, op := range ops {
		p := r.ByOp[domain.OpType(op)]
		b.WriteString(sprintf(`<tr><td>%s</td><td class="num">%s</td>`, esc(op), esc(fmtInt(p.Count))))
		for _, v := range []float64{p.P50, p.P90, p.P95, p.P99, p.P999, p.P9999, p.Max} {
			b.WriteString(sprintf(`<td class="num">%s</td>`, esc(fmtFloat(v))))
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</tbody></table></div><p class="muted" style="font-size:.8rem;margin:8px 0 0">Latencies in milliseconds.</p></div>`)
	return b.String()
}

// timeseriesHTML renders the throughput-over-time and latency-over-time charts.
func timeseriesHTML(snaps []domain.LatencySnapshot) string {
	pts := aggregate(snaps)
	if len(pts) == 0 {
		return ""
	}
	start := pts[0].T
	var tput []xy
	var p50, p99, p999 []xy
	for _, p := range pts {
		sec := p.T.Sub(start).Seconds()
		tput = append(tput, xy{sec, p.Throughput})
		p50 = append(p50, xy{sec, p.P50})
		p99 = append(p99, xy{sec, p.P99})
		p999 = append(p999, xy{sec, p.P999})
	}
	var b strings.Builder
	b.WriteString(`<h2>Throughput over time</h2><div class="card">`)
	b.WriteString(lineChartSVG("ops/s", []lineSeries{{Name: "throughput", Color: "var(--series-1)", Pts: tput}}))
	b.WriteString(`<p class="muted" style="font-size:.8rem;margin:8px 0 0">Completed ops/sec per interval.</p></div>`)
	b.WriteString(`<h2>Latency over time</h2><div class="card">`)
	lat := []lineSeries{
		{Name: "p50", Color: "var(--series-1)", Pts: p50},
		{Name: "p99", Color: "var(--series-2)", Pts: p99},
		{Name: "p99.9", Color: "var(--series-3)", Pts: p999},
	}
	b.WriteString(lineChartSVG("ms", lat))
	b.WriteString(`<div class="legend">`)
	for _, s := range lat {
		b.WriteString(sprintf(`<span><span class="swatch" style="background:%s"></span>%s</span>`, s.Color, esc(s.Name)))
	}
	b.WriteString(`</div></div>`)
	return b.String()
}

// correctnessHTML renders the verdict banner and verification details.
func correctnessHTML(v domain.VerifyReport) string {
	cls, label := "unknown", "UNKNOWN"
	switch v.Verdict {
	case domain.VerdictPass:
		cls, label = "pass", "PASS"
	case domain.VerdictFail:
		cls, label = "fail", "FAIL"
	}
	var b strings.Builder
	b.WriteString(`<h2>Correctness</h2>`)
	b.WriteString(sprintf(`<div class="banner %s">Verdict: %s &middot; model %s</div>`,
		cls, esc(label), esc(string(v.Model))))
	b.WriteString(`<div class="card"><div class="stats">`)
	b.WriteString(statHTML("Checked", fmtInt(v.Checked)))
	b.WriteString(statHTML("Lost", fmtInt(v.Lost)))
	b.WriteString(statHTML("Duplicated", fmtInt(v.Duplicated)))
	b.WriteString(statHTML("Ordering viol.", fmtInt(v.OrderingViol)))
	if v.ConvergedIn > 0 {
		b.WriteString(statHTML("Converged in", fmtDur(v.ConvergedIn)))
	}
	if v.MaxStaleness > 0 {
		b.WriteString(statHTML("Max staleness", fmtDur(v.MaxStaleness)))
	}
	b.WriteString(`</div>`)
	if len(v.Anomalies) > 0 {
		b.WriteString(`<ul class="anom">`)
		for _, a := range v.Anomalies {
			line := sprintf("<strong>%s</strong>: %s", esc(a.Kind), esc(a.Detail))
			if len(a.Witness) > 0 {
				line += sprintf(` <code>%s</code>`, esc(strings.Join(a.Witness, ", ")))
			}
			b.WriteString("<li>" + line + "</li>")
		}
		b.WriteString(`</ul>`)
	} else {
		b.WriteString(`<p class="muted" style="font-size:.85rem;margin:4px 0 0">No anomalies reported.</p>`)
	}
	b.WriteString(`</div>`)
	return b.String()
}
