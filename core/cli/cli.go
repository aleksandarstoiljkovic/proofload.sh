// Package cli is the assembly layer: it wires the frozen ports (driver,
// provisioner) and the core packages (config, workload, schedule, metrics,
// runner, emit) into the command-line interface that every target's engine
// binary shares. Each engine's main.go registers its driver, embeds its
// declarative assets, and calls Execute.
package cli

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/proofload/proofload/core/driver"
	"github.com/spf13/cobra"
)

// version is stamped into run manifests; overridden at build time via -ldflags.
var version = "0.1.0-dev"

// infraDirName is the per-target directory holding a deployment/test cluster
// bundle (docker-compose.yml + proofload-cluster.json). When present and
// --provision compose is used, the compose provisioner runs it verbatim.
const infraDirName = "infrastructure"

// Engine describes one target's binary: its name, its driver, and the embedded
// declarative assets (target.yaml, workloads/*.yaml, schema/*).
type Engine struct {
	Name   string
	Driver driver.Driver
	Assets fs.FS
}

// Execute builds and runs the cobra command tree for one engine.
func Execute(e Engine) error {
	if e.Driver == nil {
		return fmt.Errorf("cli: engine %q has no driver", e.Name)
	}
	driver.Register(e.Driver)

	root := &cobra.Command{
		Use:           "proofload-" + e.Name,
		Short:         "proofload load/correctness engine for " + e.Name,
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	root.AddCommand(runCmd(e), workerCmd(e), schemaCmd(e), listWorkloadsCmd(e), reportCmd(), ingestCmd(), runsCmd())
	return root.Execute()
}

// materialize copies an embedded asset tree to a temporary directory so the
// path-based config loader can read it, and returns the dir plus a cleanup func.
func materialize(assets fs.FS) (dir string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "proofload-assets-")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }

	walkErr := fs.WalkDir(assets, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		dst := filepath.Join(dir, p)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		return copyFile(assets, p, dst)
	})
	if walkErr != nil {
		cleanup()
		return "", nil, walkErr
	}
	return dir, cleanup, nil
}

func copyFile(assets fs.FS, src, dst string) error {
	in, err := assets.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
