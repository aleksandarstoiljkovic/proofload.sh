// Package deps pins the third-party modules proofload depends on as direct
// requirements via blank imports. This exists so parallel work can import these
// libraries without needing to edit go.mod (which would race). It is not
// imported by any real code path.
package deps

import (
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/HdrHistogram/hdrhistogram-go"
	_ "github.com/anishathalye/porcupine"
	_ "github.com/gocql/gocql"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/knadh/koanf/parsers/yaml"
	_ "github.com/knadh/koanf/providers/confmap"
	_ "github.com/knadh/koanf/providers/env/v2"
	_ "github.com/knadh/koanf/providers/file"
	_ "github.com/knadh/koanf/v2"
	_ "github.com/marcboeker/go-duckdb/v2"
	_ "github.com/parquet-go/parquet-go"
	_ "github.com/redis/go-redis/v9"
	_ "github.com/spf13/cobra"
	_ "github.com/twmb/franz-go/pkg/kgo"
)
