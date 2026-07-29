package warehouse

import (
	"database/sql"
	"os"
	"path/filepath"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/emit"
)

// pathsFor reconstructs the canonical emit.Paths for a run directory of the form
// <base>/results/<target>/<runID>, reusing emit.Layout rather than hardcoding
// artifact basenames.
func pathsFor(runDir string) emit.Paths {
	clean := filepath.Clean(runDir)
	id := domain.RunID(filepath.Base(clean))
	target := filepath.Base(filepath.Dir(clean))
	base := filepath.Dir(filepath.Dir(filepath.Dir(clean)))
	return emit.Layout(base, target, id)
}

// Ingest reads one run's manifest and summary (and verify.json if present) and
// upserts a single row into the runs table. It is idempotent: re-ingesting the
// same run replaces the existing row rather than duplicating it.
func (w *Warehouse) Ingest(runDir string) error {
	p := pathsFor(runDir)
	m, err := emit.ReadManifest(p)
	if err != nil {
		return err
	}
	r, err := emit.ReadSummary(p)
	if err != nil {
		return err
	}

	verdict := verdictOf(p)

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM runs WHERE run_id = ?`, string(m.RunID)); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO runs
		(run_id, target, workload, mode, created_at, throughput, p50, p99, p999, max,
		 total, errors, client_bound, verify_verdict)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(m.RunID), m.Target, m.Workload, string(m.Mode), m.CreatedAt,
		r.Throughput, r.Overall.P50, r.Overall.P99, r.Overall.P999, r.Overall.Max,
		r.Total, r.Errors, r.ClientBound, verdict,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// verdictOf returns the verification verdict as a nullable string: NULL when no
// verify.json is present or it cannot be read.
func verdictOf(p emit.Paths) sql.NullString {
	if _, err := os.Stat(p.VerifyJSON); err != nil {
		return sql.NullString{}
	}
	v, err := emit.ReadVerify(p)
	if err != nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(v.Verdict), Valid: true}
}
