package verify_test

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/verify"
)

// writeHistory persists events to a fresh NDJSON file and parses them back.
func writeHistory(t *testing.T, events []verify.Event) *verify.History {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.ndjson")
	w, err := verify.NewHistoryWriter(path)
	if err != nil {
		t.Fatalf("NewHistoryWriter: %v", err)
	}
	for _, e := range events {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h, err := verify.ReadHistory(path)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	return h
}

// TestHistoryRoundTrip writes a mixed history and asserts ReadHistory recovers
// every field, in order, indexed by process and key.
func TestHistoryRoundTrip(t *testing.T) {
	in := []verify.Event{
		{Process: 0, F: "write", Key: 1, WVal: 111, Invoke: 10, Complete: 20, OK: true},
		{Process: 1, F: "read", Key: 1, RVal: 111, Invoke: 30, Complete: 40, OK: true},
		{Process: 0, F: "append", Key: 2, WVal: 7, RList: []int64{7}, Invoke: 50, Complete: 60, OK: true},
	}
	h := writeHistory(t, in)

	if got := len(h.All()); got != len(in) {
		t.Fatalf("All() has %d events, want %d", got, len(in))
	}
	if got := h.All()[0]; !reflect.DeepEqual(got, in[0]) {
		t.Errorf("event 0 = %+v, want %+v", got, in[0])
	}
	if got := h.All()[2]; !reflect.DeepEqual(got, in[2]) {
		t.Errorf("event 2 = %+v, want %+v", got, in[2])
	}
	if got := len(h.ByProcess(0)); got != 2 {
		t.Errorf("ByProcess(0) = %d events, want 2", got)
	}
	if got := len(h.ByKey(1)); got != 2 {
		t.Errorf("ByKey(1) = %d events, want 2", got)
	}
	if got := h.Keys(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("Keys() = %v, want [1 2]", got)
	}
}

// TestHistoryConcurrentAppend has many goroutines append to one writer, then
// confirms every event survives the round-trip (mutex-serialized writes).
func TestHistoryConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.ndjson")
	w, err := verify.NewHistoryWriter(path)
	if err != nil {
		t.Fatalf("NewHistoryWriter: %v", err)
	}

	const procs, perProc = 16, 64
	var wg sync.WaitGroup
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProc; i++ {
				e := verify.Event{Process: p, F: "write", Key: int64(i), WVal: uint64(p), OK: true}
				if err := w.Append(e); err != nil {
					t.Errorf("Append: %v", err)
				}
			}
		}(p)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h, err := verify.ReadHistory(path)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if want := procs * perProc; len(h.All()) != want {
		t.Fatalf("recovered %d events, want %d", len(h.All()), want)
	}
	for p := 0; p < procs; p++ {
		if got := len(h.ByProcess(p)); got != perProc {
			t.Errorf("ByProcess(%d) = %d, want %d", p, got, perProc)
		}
	}
}

// TestRecorderObserveClassifies drives HistoryRecorder.Observe with a write, a
// read, and an append and asserts the family and value fields are filled.
func TestRecorderObserveClassifies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rec.ndjson")
	w, err := verify.NewHistoryWriter(path)
	if err != nil {
		t.Fatalf("NewHistoryWriter: %v", err)
	}
	rec := verify.NewHistoryRecorder(3, w)
	now := time.Unix(0, 1000)
	later := time.Unix(0, 2000)

	writeVal := []byte("payload-v1")
	rec.Observe(
		domain.Operation{Type: "write", Key: 1, Value: writeVal},
		domain.OpResult{}, now, later)
	rec.Observe(
		domain.Operation{Type: "read", Key: 1},
		domain.OpResult{Observed: writeVal}, now, later)
	rec.Observe(
		domain.Operation{Type: "append", Key: 2, Value: []byte("elem")},
		domain.OpResult{Observed: []int64{5, 9}}, now, later)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h, err := verify.ReadHistory(path)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	events := h.All()
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}

	wantW := verify.Checksum(writeVal)
	if e := events[0]; e.F != "write" || e.WVal != wantW || e.RVal != 0 {
		t.Errorf("write event = %+v, want F=write WVal=%d RVal=0", e, wantW)
	}
	if e := events[1]; e.F != "read" || e.RVal != wantW || e.WVal != 0 {
		t.Errorf("read event = %+v, want F=read RVal=%d WVal=0", e, wantW)
	}
	if e := events[2]; e.F != "append" || e.WVal != verify.Checksum([]byte("elem")) ||
		len(e.RList) != 2 || e.RList[0] != 5 || e.RList[1] != 9 {
		t.Errorf("append event = %+v, want F=append with RList [5 9]", e)
	}
	if !events[0].OK {
		t.Errorf("write with nil Err should be OK")
	}
}
