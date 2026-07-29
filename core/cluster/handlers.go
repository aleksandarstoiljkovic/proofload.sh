package cluster

import (
	"encoding/json"
	"net/http"
	"time"
)

// handleJoin registers a worker, then long-polls until the join barrier opens
// and replies with the assigned id, the spec, and the synchronized T0.
func (c *Coordinator) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad join body", http.StatusBadRequest)
		return
	}
	id, ok := c.register(req.RequestedID)
	if !ok {
		http.Error(w, "cluster full: late join rejected", http.StatusConflict)
		return
	}
	select {
	case <-c.joinedAll:
		writeJSON(w, joinResponse{WorkerID: id, Spec: c.spec, StartAt: c.startAt})
	case <-r.Context().Done():
	case <-c.stop:
		http.Error(w, "coordinator shutting down", http.StatusServiceUnavailable)
	}
}

// register assigns a WorkerID for a join request under lock and opens the
// barrier once every slot is filled. It returns false when the cluster is full.
func (c *Coordinator) register(requested int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := -1
	if requested >= 0 && requested < c.workerCount && !c.joined[requested] {
		id = requested // honor a free, in-range preassignment
	} else {
		for i := 0; i < c.workerCount; i++ { // collision or auto: lowest free slot
			if !c.joined[i] {
				id = i
				break
			}
		}
	}
	if id < 0 {
		return -1, false
	}
	c.joined[id] = true
	if len(c.joined) == c.workerCount {
		c.joinOnce.Do(func() {
			c.startAt = time.Now().Add(c.startLead())
			close(c.joinedAll)
		})
	}
	return id, true
}

// handleSubmit records a worker's report and opaque histogram blob, rejecting
// any id that never joined.
func (c *Coordinator) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad submit body", http.StatusBadRequest)
		return
	}
	if !c.recordSubmit(req) {
		http.Error(w, "unknown worker id", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// recordSubmit stores a submission under lock and opens the completion barrier
// once every worker has reported. It returns false for an unknown WorkerID.
func (c *Coordinator) recordSubmit(req submitRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.joined[req.Report.WorkerID] {
		return false
	}
	c.reports[req.Report.WorkerID] = req.Report
	c.histos[req.Report.WorkerID] = req.Histogram
	if len(c.reports) == c.workerCount {
		c.submitOnce.Do(func() { close(c.submittedAll) })
	}
	return true
}

// result snapshots the collected reports and blobs ordered by WorkerID.
func (c *Coordinator) result() Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := Result{}
	for i := 0; i < c.workerCount; i++ {
		rep, ok := c.reports[i]
		if !ok {
			continue
		}
		res.Reports = append(res.Reports, rep)
		res.Histograms = append(res.Histograms, c.histos[i])
	}
	return res
}
