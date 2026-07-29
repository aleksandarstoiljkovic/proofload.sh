// Command proofload-redis is the Redis load/correctness engine. It registers the
// go-redis-based driver, embeds its declarative assets, and delegates all command
// handling to the shared core/cli assembly layer.
package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/proofload/proofload/core/cli"
	_ "github.com/proofload/proofload/core/provision/backends" // registers compose + kubernetes
	"github.com/proofload/proofload/targets/redis/redisdriver"
)

//go:embed target.yaml
//go:embed workloads
var assets embed.FS

func main() {
	err := cli.Execute(cli.Engine{
		Name:   "redis",
		Driver: redisdriver.New(),
		Assets: assets,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
