// Command proofload-kafka is the Apache Kafka load/correctness engine. It
// registers the franz-go-based driver, embeds its declarative assets, and
// delegates all command handling to the shared core/cli assembly layer.
package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/proofload/proofload/core/cli"
	_ "github.com/proofload/proofload/core/provision/backends" // registers compose + kubernetes
	"github.com/proofload/proofload/targets/kafka/kafkadriver"
)

//go:embed target.yaml
//go:embed workloads
var assets embed.FS

func main() {
	err := cli.Execute(cli.Engine{
		Name:   "kafka",
		Driver: kafkadriver.New(),
		Assets: assets,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
