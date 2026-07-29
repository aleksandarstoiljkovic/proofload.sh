package verify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// WriteRecord is one committed write the load engine recorded: the key, the
// Checksum of the value written, and the per-key monotonic Seq. The runner MUST
// append exactly one record per COMMITTED write and compute Checksum with this
// package's Checksum function.
type WriteRecord struct {
	Key      int64  `json:"key"`
	Checksum uint64 `json:"checksum"`
	Seq      int64  `json:"seq"`
}

// Log is a parsed expectation log reduced to last-write-wins per key by Seq.
type Log struct {
	expected map[int64]WriteRecord
}

// ReadLog parses an append-only NDJSON expectation log, keeping the
// highest-Seq record per key (last write wins). Shards written concurrently by
// many workers can simply be concatenated before reading.
func ReadLog(path string) (*Log, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readLog(f)
}

func readLog(r io.Reader) (*Log, error) {
	expected := make(map[int64]WriteRecord)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for line := 1; sc.Scan(); line++ {
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var rec WriteRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("verify: log line %d: %w", line, err)
		}
		if cur, ok := expected[rec.Key]; !ok || rec.Seq >= cur.Seq {
			expected[rec.Key] = rec
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("verify: read log: %w", err)
	}
	return &Log{expected: expected}, nil
}

// Expected returns the final expected record per key. The returned map is a
// fresh copy; callers may mutate it freely.
func (l *Log) Expected() map[int64]WriteRecord {
	out := make(map[int64]WriteRecord, len(l.expected))
	for k, v := range l.expected {
		out[k] = v
	}
	return out
}
