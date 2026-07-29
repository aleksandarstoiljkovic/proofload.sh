package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/proofload/proofload/core/report"
	"github.com/proofload/proofload/core/warehouse"
	"github.com/spf13/cobra"
)

// reportCmd renders a self-contained HTML report for one run directory.
func reportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "report <run-dir>",
		Short: "Render a self-contained HTML report for a run directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := report.WriteReport(args[0])
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), path)
			return nil
		},
	}
}

// ingestCmd loads one run directory into the DuckDB cross-run warehouse.
func ingestCmd() *cobra.Command {
	var whPath string
	cmd := &cobra.Command{
		Use:   "ingest <run-dir>",
		Short: "Ingest a run into the DuckDB warehouse for cross-run analytics",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if dir := filepath.Dir(whPath); dir != "" && dir != "." {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return err
				}
			}
			wh, err := warehouse.Open(whPath)
			if err != nil {
				return err
			}
			defer wh.Close()
			if err := wh.Ingest(args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ingested %s into %s\n", args[0], whPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&whPath, "warehouse", "warehouse/proofload.duckdb", "DuckDB warehouse file")
	return cmd
}

// runsCmd lists recorded runs from the warehouse for cross-run comparison.
func runsCmd() *cobra.Command {
	var (
		whPath   string
		target   string
		workload string
		limit    int
	)
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List runs recorded in the warehouse (most recent first)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			wh, err := warehouse.Open(whPath)
			if err != nil {
				return err
			}
			defer wh.Close()
			rows, err := wh.Runs(target, workload, limit)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "%-40s %-12s %-18s %10s %8s %8s %8s\n",
				"RUN", "TARGET", "WORKLOAD", "TPUT", "P99ms", "ERRORS", "VERDICT")
			for _, r := range rows {
				fmt.Fprintf(out, "%-40s %-12s %-18s %10.0f %8.2f %8d %8s\n",
					r.RunID, r.Target, r.Workload, r.Throughput, r.P99, r.Errors, verdictOr(r.VerifyVerdict))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&whPath, "warehouse", "warehouse/proofload.duckdb", "DuckDB warehouse file")
	cmd.Flags().StringVar(&target, "target", "", "filter by target")
	cmd.Flags().StringVar(&workload, "workload", "", "filter by workload")
	cmd.Flags().IntVar(&limit, "limit", 20, "max rows")
	return cmd
}

func verdictOr(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
