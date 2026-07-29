package cli

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/proofload/proofload/core/config"
	"github.com/spf13/cobra"
)

// schemaCmd applies the target's schema without generating load.
func schemaCmd(e Engine) *cobra.Command {
	var (
		workloadName string
		endpoints    []string
		consistency  string
	)
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Apply the target schema for a workload",
		RunE: func(cmd *cobra.Command, _ []string) error {
			dir, cleanup, err := materialize(e.Assets)
			if err != nil {
				return err
			}
			defer cleanup()
			resolved, err := config.Resolve(config.ResolveOptions{
				TargetPath:   filepath.Join(dir, "target.yaml"),
				WorkloadPath: filepath.Join(dir, "workloads", workloadName+".yaml"),
				Endpoints:    endpoints,
				Consistency:  consistency,
			})
			if err != nil {
				return err
			}
			if len(resolved.Driver.Endpoints) == 0 {
				return fmt.Errorf("no endpoints: pass --endpoints host:port")
			}
			if err := e.Driver.Schema(context.Background(), resolved.Driver, resolved.Workload); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "schema applied")
			return nil
		},
	}
	cmd.Flags().StringVar(&workloadName, "workload", "", "workload name")
	cmd.Flags().StringSliceVar(&endpoints, "endpoints", nil, "client endpoints host:port")
	cmd.Flags().StringVar(&consistency, "consistency", "", "consistency/isolation level")
	_ = cmd.MarkFlagRequired("workload")
	_ = cmd.MarkFlagRequired("endpoints")
	return cmd
}

// listWorkloadsCmd prints the workloads embedded in this engine binary.
func listWorkloadsCmd(e Engine) *cobra.Command {
	return &cobra.Command{
		Use:   "list-workloads",
		Short: "List available workloads",
		RunE: func(cmd *cobra.Command, _ []string) error {
			entries, err := fs.ReadDir(e.Assets, "workloads")
			if err != nil {
				return err
			}
			var names []string
			for _, en := range entries {
				if n := strings.TrimSuffix(en.Name(), ".yaml"); n != en.Name() {
					names = append(names, n)
				}
			}
			sort.Strings(names)
			for _, n := range names {
				fmt.Fprintln(cmd.OutOrStdout(), n)
			}
			return nil
		},
	}
}
