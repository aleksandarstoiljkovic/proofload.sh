package verify_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/proofload/proofload/core/verify"
)

// TestLogRoundTripLastWriteWins writes duplicate keys with differing Seq (some
// out of order) and asserts ReadLog keeps the highest-Seq record per key.
func TestLogRoundTripLastWriteWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "expect.ndjson")
	records := []verify.WriteRecord{
		{Key: 1, Checksum: 100, Seq: 1},
		{Key: 2, Checksum: 200, Seq: 1},
		{Key: 1, Checksum: 111, Seq: 3}, // newer wins for key 1
		{Key: 1, Checksum: 999, Seq: 2}, // stale, arrives after the newer one
		{Key: 2, Checksum: 222, Seq: 2}, // newer wins for key 2
	}
	writeRecords(t, path, records)

	log, err := verify.ReadLog(path)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	got := log.Expected()
	if len(got) != 2 {
		t.Fatalf("Expected() has %d keys, want 2", len(got))
	}
	if r := got[1]; r.Seq != 3 || r.Checksum != 111 {
		t.Errorf("key 1 = %+v, want Seq 3 / Checksum 111", r)
	}
	if r := got[2]; r.Seq != 2 || r.Checksum != 222 {
		t.Errorf("key 2 = %+v, want Seq 2 / Checksum 222", r)
	}
}

// TestLogConcurrentRecord has many goroutines write disjoint keys to one
// LogWriter, then confirms ReadLog recovers every record.
func TestLogConcurrentRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shard.ndjson")
	w, err := verify.NewLogWriter(path)
	if err != nil {
		t.Fatalf("NewLogWriter: %v", err)
	}

	const goroutines, perGoroutine = 16, 64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := int64(g*perGoroutine + i)
				if err := w.Record(verify.WriteRecord{Key: key, Checksum: uint64(key), Seq: 1}); err != nil {
					t.Errorf("Record(%d): %v", key, err)
				}
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	log, err := verify.ReadLog(path)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	got := log.Expected()
	if want := goroutines * perGoroutine; len(got) != want {
		t.Fatalf("recovered %d records, want %d", len(got), want)
	}
	for key, r := range got {
		if r.Checksum != uint64(key) {
			t.Fatalf("key %d checksum %d, want %d (corrupted line?)", key, r.Checksum, key)
		}
	}
}

func writeRecords(t *testing.T, path string, records []verify.WriteRecord) {
	t.Helper()
	w, err := verify.NewLogWriter(path)
	if err != nil {
		t.Fatalf("NewLogWriter: %v", err)
	}
	for _, r := range records {
		if err := w.Record(r); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
