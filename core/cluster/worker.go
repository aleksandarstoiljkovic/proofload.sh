package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Worker is a client-side handle to the coordinator for one load-client machine.
// It is single-run: Join once, then Submit once.
type Worker struct {
	coordURL string
	id       int
	client   *http.Client
}

// NewWorker creates a worker that talks to the coordinator at coordAddr (with or
// without an http:// scheme). Pass id >= 0 to request a specific WorkerID, or a
// negative id to have the coordinator assign one.
func NewWorker(coordAddr string, id int) *Worker {
	return &Worker{
		coordURL: normalizeURL(coordAddr),
		id:       id,
		// No client Timeout: /join long-polls until the barrier opens; callers
		// bound the wait through the context passed to Join/Submit.
		client: &http.Client{},
	}
}

// ID returns the worker's assigned WorkerID (valid after Join succeeds). The
// caller uses it both to shard the keyspace and to stamp its WorkerReport.
func (w *Worker) ID() int { return w.id }

// Join registers the worker and blocks until every worker has joined, then
// returns the shared RunSpec and the synchronized start time T0 at which the
// measure phase should begin. It also records the assigned id (see ID).
func (w *Worker) Join(ctx context.Context) (RunSpec, time.Time, error) {
	var resp joinResponse
	if err := w.post(ctx, "/join", joinRequest{RequestedID: w.id}, &resp); err != nil {
		return RunSpec{}, time.Time{}, err
	}
	w.id = resp.WorkerID
	return resp.Spec, resp.StartAt, nil
}

// Submit posts the worker's final report and its opaque encoded-histogram blob
// to the coordinator.
func (w *Worker) Submit(ctx context.Context, r WorkerReport, histogram []byte) error {
	return w.post(ctx, "/submit", submitRequest{Report: r, Histogram: histogram}, nil)
}

// post marshals in, POSTs it to path, and decodes any reply into out (skip when
// nil). A non-2xx status becomes an error carrying the server message.
func (w *Worker) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("cluster: marshal %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.coordURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cluster: request %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("cluster: post %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("cluster: %s: %s: %s", path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("cluster: decode %s: %w", path, err)
		}
	}
	return nil
}

// normalizeURL ensures coordAddr has a scheme and no trailing slash.
func normalizeURL(coordAddr string) string {
	addr := strings.TrimRight(coordAddr, "/")
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}
