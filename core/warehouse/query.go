package warehouse

import (
	"database/sql"
	"strings"
	"time"
)

// RunRow is one run's headline facts as stored in the warehouse. VerifyVerdict
// is empty when the run had no verification report.
type RunRow struct {
	RunID         string
	Target        string
	Workload      string
	Mode          string
	CreatedAt     time.Time
	Throughput    float64
	P50           float64
	P99           float64
	P999          float64
	Max           float64
	Total         int64
	Errors        int64
	ClientBound   bool
	VerifyVerdict string
}

// Runs returns stored runs most recent first (by created_at, then run_id). The
// target and workload filters are applied only when non-empty; a limit <= 0
// returns all matching rows.
func (w *Warehouse) Runs(target, workload string, limit int) ([]RunRow, error) {
	var where []string
	var args []any
	if target != "" {
		where = append(where, "target = ?")
		args = append(args, target)
	}
	if workload != "" {
		where = append(where, "workload = ?")
		args = append(args, workload)
	}

	q := `SELECT run_id, target, workload, mode, created_at, throughput,
		p50, p99, p999, max, total, errors, client_bound, verify_verdict FROM runs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC, run_id DESC"
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := w.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RunRow
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// scanRun reads a single runs row, translating the nullable verdict column.
func scanRun(rows *sql.Rows) (RunRow, error) {
	var r RunRow
	var verdict sql.NullString
	err := rows.Scan(&r.RunID, &r.Target, &r.Workload, &r.Mode, &r.CreatedAt,
		&r.Throughput, &r.P50, &r.P99, &r.P999, &r.Max,
		&r.Total, &r.Errors, &r.ClientBound, &verdict)
	if err != nil {
		return RunRow{}, err
	}
	r.VerifyVerdict = verdict.String
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}
