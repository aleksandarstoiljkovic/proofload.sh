package metrics

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/proofload/proofload/core/domain"
)

// wireEnvelope is the on-the-wire form of a Recorder: the config plus each
// histogram serialized with the library's own lossless V2 compressed encoding
// (itself base64). The whole envelope is base64-wrapped by Encode so the result
// is a single flat base64 blob suitable for shipping to a coordinator.
type wireEnvelope struct {
	High     int64                    `json:"high_ns"`
	Sig      int                      `json:"sig"`
	Overall  string                   `json:"overall"`
	ByOp     map[domain.OpType]string `json:"by_op"`
	ErrByOp  map[domain.OpType]int64  `json:"err_by_op"`
	ErrTotal int64                    `json:"err_total"`
}

// Encode serializes the Recorder's merged histograms for distributed merge.
// The per-histogram payloads use hdrhistogram's V2 compressed encoding, which
// is lossless, so a Decode+Merge on a coordinator reconstructs the exact
// distribution.
func (r *Recorder) Encode() ([]byte, error) {
	m := r.collect()

	overall, err := m.overall.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		return nil, fmt.Errorf("encode overall: %w", err)
	}
	env := wireEnvelope{
		High:     r.high,
		Sig:      r.sig,
		Overall:  string(overall),
		ByOp:     make(map[domain.OpType]string, len(m.byOp)),
		ErrByOp:  m.errByOp,
		ErrTotal: m.errTotal,
	}
	for op, h := range m.byOp {
		enc, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
		if err != nil {
			return nil, fmt.Errorf("encode op %q: %w", op, err)
		}
		env.ByOp[op] = string(enc)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	return []byte(base64.StdEncoding.EncodeToString(raw)), nil
}

// Decode reconstructs a Recorder from Encode output. The result holds the
// decoded histograms as one synthetic Local, ready to be Merged or Snapshotted.
func Decode(b []byte) (*Recorder, error) {
	raw, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}

	overall, err := hdrhistogram.Decode([]byte(env.Overall))
	if err != nil {
		return nil, fmt.Errorf("decode overall: %w", err)
	}
	byOp := make(map[domain.OpType]*hdrhistogram.Histogram, len(env.ByOp))
	for op, s := range env.ByOp {
		h, err := hdrhistogram.Decode([]byte(s))
		if err != nil {
			return nil, fmt.Errorf("decode op %q: %w", op, err)
		}
		byOp[op] = h
	}

	sig := env.Sig
	if sig < 1 || sig > 5 {
		sig = defaultSigFigs
	}
	errByOp := env.ErrByOp
	if errByOp == nil {
		errByOp = make(map[domain.OpType]int64)
	}
	r := &Recorder{
		high:     env.High,
		sig:      sig,
		prevSnap: make(map[domain.OpType]*hdrhistogram.Snapshot),
		prevErr:  make(map[domain.OpType]int64),
		locals: []*Local{{
			high:     env.High,
			sig:      sig,
			overall:  overall,
			byOp:     byOp,
			errByOp:  errByOp,
			errTotal: env.ErrTotal,
		}},
	}
	return r, nil
}
