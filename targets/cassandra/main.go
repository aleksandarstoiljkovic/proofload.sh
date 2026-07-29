// Command proofload-cassandra is the Apache Cassandra load/correctness engine.
// It registers the gocql-based driver, embeds its declarative assets, and
// delegates all command handling to the shared core/cli assembly layer.
package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/proofload/proofload/core/cli"
	_ "github.com/proofload/proofload/core/provision/backends" // registers compose + kubernetes
	"github.com/proofload/proofload/targets/cassandra/cassandradriver"
)

//go:embed target.yaml
//go:embed workloads
//go:embed schema
var assets embed.FS

func main() {
	err := cli.Execute(cli.Engine{
		Name:   "cassandra",
		Driver: cassandradriver.New(),
		Assets: assets,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
