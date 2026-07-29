package verify

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// LogWriter appends WriteRecords to an NDJSON expectation log — one JSON object
// per line, e.g. {"key":7,"checksum":12345,"seq":3}. The format is append-only
// so distributed workers can each open their own shard file and write
// independently. Record is safe for concurrent callers within one process.
type LogWriter struct {
	mu  sync.Mutex
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder
}

// NewLogWriter opens (creating if needed) an append-mode log at path.
func NewLogWriter(path string) (*LogWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(f)
	return &LogWriter{f: f, bw: bw, enc: json.NewEncoder(bw)}, nil
}

// Record appends one WriteRecord as a single NDJSON line. json.Encoder writes a
// trailing newline, so records are newline-delimited. Safe for concurrent use.
func (w *LogWriter) Record(r WriteRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(r)
}

// Close flushes buffered records and closes the underlying file.
func (w *LogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
