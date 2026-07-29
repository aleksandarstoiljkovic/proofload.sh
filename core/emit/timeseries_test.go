package emit

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
)

func sampleSnapshots() []domain.LatencySnapshot {
	base := time.UnixMicro(1_700_000_000_000_000).UTC()
	return []domain.LatencySnapshot{
		{
			T: base, OpType: "read", Throughput: 4200.5, InFlight: 64, Errors: 0,
			Pct: domain.Percentiles{Count: 4200, Mean: 1.1, P50: 0.9, P90: 2.0, P95: 3.0, P99: 8.0, P999: 20.0, P9999: 40.0, Max: 55.0},
		},
		{
			T: base.Add(time.Second), OpType: "write", Throughput: 1100.0, InFlight: 32, Errors: 3,
			Pct: domain.Percentiles{Count: 1100, Mean: 2.5, P50: 2.0, P90: 5.0, P95: 7.0, P99: 18.0, P999: 44.0, P9999: 88.0, Max: 120.0},
		},
	}
}

func TestTimeseriesParquetRoundTrip(t *testing.T) {
	p := Layout(t.TempDir(), "postgres", "run-1")
	want := sampleSnapshots()

	if err := WriteTimeseriesParquet(p, want); err != nil {
		t.Fatalf("WriteTimeseriesParquet: %v", err)
	}
	got, err := ReadTimeseriesParquet(p)
	if err != nil {
		t.Fatalf("ReadTimeseriesParquet: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestTimeseriesNDJSONOneObjectPerLine(t *testing.T) {
	p := Layout(t.TempDir(), "postgres", "run-1")
	snaps := sampleSnapshots()

	if err := WriteTimeseriesNDJSON(p, snaps); err != nil {
		t.Fatalf("WriteTimeseriesNDJSON: %v", err)
	}
	data, err := os.ReadFile(p.TimeseriesNDJSON)
	if err != nil {
		t.Fatalf("read ndjson: %v", err)
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != len(snaps) {
		t.Fatalf("got %d lines, want %d", len(lines), len(snaps))
	}
	for i, line := range lines {
		var snap domain.LatencySnapshot
		if err := json.Unmarshal([]byte(line), &snap); err != nil {
			t.Fatalf("line %d is not a JSON object: %v", i, err)
		}
	}
}
