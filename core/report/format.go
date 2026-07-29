package report

import (
	"fmt"
	"html"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sortedKeys returns the keys of a string map in ascending order (for
// deterministic rendering).
func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// esc HTML-escapes an arbitrary string for safe inclusion in element text or a
// double-quoted attribute.
func esc(s string) string { return html.EscapeString(s) }

// fmtInt renders an integer with thousands separators (e.g. 1234567 -> "1,234,567").
func fmtInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// fmtFloat renders a float with adaptive precision: more decimals for small
// magnitudes so sub-millisecond latencies stay legible.
func fmtFloat(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	switch a := math.Abs(v); {
	case a == 0:
		return "0"
	case a < 1:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case a < 100:
		return strconv.FormatFloat(v, 'f', 2, 64)
	case a < 10000:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return fmtInt(int64(math.Round(v)))
	}
}

// fmtMs renders a millisecond value with a trailing unit.
func fmtMs(v float64) string { return fmtFloat(v) + " ms" }

// fmtDur renders a duration compactly, preferring whole seconds when exact.
func fmtDur(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	return d.String()
}

// fmtTime renders a timestamp in UTC, second precision.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04:05 MST")
}

// coord formats a float for an SVG coordinate attribute (two decimals, no unit).
func coord(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }

// sprintf is a thin alias to keep section builders terse.
func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }
