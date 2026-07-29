package emit

import (
	"bytes"
	"encoding/json"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/proofload/proofload/core/domain"
)

// tsRow is the flat Parquet schema derived from domain.LatencySnapshot. The
// nested Percentiles struct is flattened into individual *_ms columns and the
// timestamp is stored as Unix microseconds (t_unix_micros) so it round-trips
// independent of location. On read, timestamps are restored in UTC.
//
//	t_unix_micros int64   -- snapshot instant, Unix microseconds
//	op            string  -- operation label (domain.OpType)
//	throughput    float64 -- completed ops/sec in the interval
//	in_flight     int64   -- outstanding ops at snapshot time
//	errors        int64   -- error count in the interval
//	count         int64   -- Percentiles.Count
//	mean_ms       float64 -- Percentiles.Mean
//	p50_ms..max_ms float64 -- Percentiles p50/p90/p95/p99/p999/p9999/max
type tsRow struct {
	TUnixMicros int64   `parquet:"t_unix_micros"`
	Op          string  `parquet:"op"`
	Throughput  float64 `parquet:"throughput"`
	InFlight    int64   `parquet:"in_flight"`
	Errors      int64   `parquet:"errors"`
	Count       int64   `parquet:"count"`
	MeanMs      float64 `parquet:"mean_ms"`
	P50Ms       float64 `parquet:"p50_ms"`
	P90Ms       float64 `parquet:"p90_ms"`
	P95Ms       float64 `parquet:"p95_ms"`
	P99Ms       float64 `parquet:"p99_ms"`
	P999Ms      float64 `parquet:"p999_ms"`
	P9999Ms     float64 `parquet:"p9999_ms"`
	MaxMs       float64 `parquet:"max_ms"`
}

func toRow(s domain.LatencySnapshot) tsRow {
	return tsRow{
		TUnixMicros: s.T.UnixMicro(),
		Op:          string(s.OpType),
		Throughput:  s.Throughput,
		InFlight:    int64(s.InFlight),
		Errors:      s.Errors,
		Count:       s.Pct.Count,
		MeanMs:      s.Pct.Mean,
		P50Ms:       s.Pct.P50,
		P90Ms:       s.Pct.P90,
		P95Ms:       s.Pct.P95,
		P99Ms:       s.Pct.P99,
		P999Ms:      s.Pct.P999,
		P9999Ms:     s.Pct.P9999,
		MaxMs:       s.Pct.Max,
	}
}

func fromRow(r tsRow) domain.LatencySnapshot {
	return domain.LatencySnapshot{
		T:          time.UnixMicro(r.TUnixMicros).UTC(),
		OpType:     domain.OpType(r.Op),
		Throughput: r.Throughput,
		InFlight:   int(r.InFlight),
		Errors:     r.Errors,
		Pct: domain.Percentiles{
			Count: r.Count,
			Mean:  r.MeanMs,
			P50:   r.P50Ms,
			P90:   r.P90Ms,
			P95:   r.P95Ms,
			P99:   r.P99Ms,
			P999:  r.P999Ms,
			P9999: r.P9999Ms,
			Max:   r.MaxMs,
		},
	}
}

// WriteTimeseriesNDJSON writes one JSON object per line (newline-delimited
// JSON), atomically. Each line is a marshaled domain.LatencySnapshot.
func WriteTimeseriesNDJSON(p Paths, snaps []domain.LatencySnapshot) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range snaps {
		if err := enc.Encode(snaps[i]); err != nil {
			return err
		}
	}
	return writeAtomic(p.TimeseriesNDJSON, buf.Bytes())
}

// WriteTimeseriesParquet writes the snapshots to a Parquet file (flat tsRow
// schema), atomically.
func WriteTimeseriesParquet(p Paths, snaps []domain.LatencySnapshot) error {
	rows := make([]tsRow, len(snaps))
	for i := range snaps {
		rows[i] = toRow(snaps[i])
	}

	var buf bytes.Buffer
	if err := parquet.Write(&buf, rows); err != nil {
		return err
	}
	return writeAtomic(p.TimeseriesParquet, buf.Bytes())
}

// ReadTimeseriesParquet loads the snapshots written by WriteTimeseriesParquet,
// restoring timestamps in UTC.
func ReadTimeseriesParquet(p Paths) ([]domain.LatencySnapshot, error) {
	data, err := os.ReadFile(p.TimeseriesParquet)
	if err != nil {
		return nil, err
	}

	rows, err := parquet.Read[tsRow](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	snaps := make([]domain.LatencySnapshot, len(rows))
	for i := range rows {
		snaps[i] = fromRow(rows[i])
	}
	return snaps, nil
}
