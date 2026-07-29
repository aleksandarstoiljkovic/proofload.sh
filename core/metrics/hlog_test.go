package metrics

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

func TestWriteHLogReadable(t *testing.T) {
	r := New(Options{})
	l := r.Local()
	opWrite := domain.OpType("write")
	for i := 1; i <= 100; i++ {
		l.Record(opRead, time.Duration(i)*time.Millisecond, false)
	}
	for i := 1; i <= 40; i++ {
		l.Record(opWrite, time.Duration(i)*time.Millisecond, false)
	}

	var buf bytes.Buffer
	if err := WriteHLog(&buf, r, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatalf("WriteHLog: %v", err)
	}

	reader := hdrhistogram.NewHistogramLogReader(&buf)
	tagCounts := make(map[string]int64)
	for {
		h, err := reader.NextIntervalHistogram()
		if err == io.EOF || (err == nil && h == nil) {
			break
		}
		if err != nil {
			t.Fatalf("read interval: %v", err)
		}
		tagCounts[h.Tag()] = h.TotalCount()
	}

	if got := tagCounts["overall"]; got != 140 {
		t.Errorf("overall count = %d, want 140", got)
	}
	if got := tagCounts["read"]; got != 100 {
		t.Errorf("read count = %d, want 100", got)
	}
	if got := tagCounts["write"]; got != 40 {
		t.Errorf("write count = %d, want 40", got)
	}
}
