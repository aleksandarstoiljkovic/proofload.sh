package verify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// HistoryWriter appends Events to an NDJSON history — one JSON object per line.
// The append-only format lets distributed workers each write their own shard
// file for later concatenation. Append is safe for concurrent callers.
type HistoryWriter struct {
	mu  sync.Mutex
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder
}

// NewHistoryWriter opens (creating if needed) an append-mode history at path.
func NewHistoryWriter(path string) (*HistoryWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(f)
	return &HistoryWriter{f: f, bw: bw, enc: json.NewEncoder(bw)}, nil
}

// Append writes one Event as a single NDJSON line. Safe for concurrent use.
func (w *HistoryWriter) Append(e Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(e)
}

// Close flushes buffered events and closes the underlying file.
func (w *HistoryWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// History is a parsed correctness history: all recorded Events in file order,
// with indexes by process and by key.
type History struct {
	events    []Event
	byProcess map[int][]Event
	byKey     map[int64][]Event
}

// ReadHistory parses an append-only NDJSON history. Event order is preserved,
// which is what the checkers rely on for real-time reasoning.
func ReadHistory(path string) (*History, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readHistory(f)
}

func readHistory(r io.Reader) (*History, error) {
	h := &History{byProcess: map[int][]Event{}, byKey: map[int64][]Event{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for line := 1; sc.Scan(); line++ {
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, fmt.Errorf("verify: history line %d: %w", line, err)
		}
		h.events = append(h.events, e)
		h.byProcess[e.Process] = append(h.byProcess[e.Process], e)
		h.byKey[e.Key] = append(h.byKey[e.Key], e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("verify: read history: %w", err)
	}
	return h, nil
}

// All returns every Event in file order.
func (h *History) All() []Event { return h.events }

// ByProcess returns the Events recorded by one process, in file order.
func (h *History) ByProcess(p int) []Event { return h.byProcess[p] }

// ByKey returns the Events touching one key, in file order.
func (h *History) ByKey(k int64) []Event { return h.byKey[k] }

// Keys returns the distinct keys in the history, sorted ascending.
func (h *History) Keys() []int64 {
	out := make([]int64, 0, len(h.byKey))
	for k := range h.byKey {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
