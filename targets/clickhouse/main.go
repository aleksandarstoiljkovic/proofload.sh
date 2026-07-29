// Command proofload-clickhouse is the ClickHouse load/correctness engine. It
// registers the clickhouse-go native-protocol driver, embeds its declarative
// assets, and delegates all command handling to the shared core/cli assembly
// layer.
package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/proofload/proofload/core/cli"
	_ "github.com/proofload/proofload/core/provision/backends" // registers compose + kubernetes
	"github.com/proofload/proofload/targets/clickhouse/clickhousedriver"
)

//go:embed target.yaml
//go:embed workloads
//go:embed schema
var assets embed.FS

func main() {
	err := cli.Execute(cli.Engine{
		Name:   "clickhouse",
		Driver: clickhousedriver.New(),
		Assets: assets,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
