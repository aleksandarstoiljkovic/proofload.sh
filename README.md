# proofload

**proofload** is a polyglot load-testing and correctness harness for databases,
key/value stores, and message brokers. One command spins up a real (optionally
clustered) system, drives it with a chosen workload, measures throughput and
full latency distributions accurately, optionally **injects node failures** and
**verifies correctness** under load, and writes a self-contained HTML report.

It exists to answer, for any supported system, questions like:

- *What's the peak throughput, and what does the latency tail look like at it?*
- *What are my p99 / p99.9 latencies at a production-like request rate?*
- *Where does it break as load ramps up?*
- *Does it lose or corrupt data when a node dies mid-run?*

One orchestrator (`proofload.sh`) dispatches to a dedicated, self-contained Go
engine per target. Adding a new system is a small, well-scoped plug-in; every
engine inherits the same measurement, correctness, fault-injection,
distributed-load, and reporting machinery.

## Supported systems

| System | Cluster it can provision | Docs |
|---|---|---|
| PostgreSQL | primary + 2 streaming replicas | [docs/postgres.md](docs/postgres.md) |
| Redis | primary + 2 replicas + 3 Sentinels | [docs/redis.md](docs/redis.md) |
| Cassandra | 3-node ring (RF=3) | [docs/cassandra.md](docs/cassandra.md) |
| Kafka | 3-broker KRaft quorum | [docs/kafka.md](docs/kafka.md) |
| ClickHouse | 2 replicas + 3 Keeper (RAFT) | [docs/clickhouse.md](docs/clickhouse.md) |

All bundles use official, maintained images.

## Highlights

- **Accurate latency** — HdrHistogram percentiles (p50…p99.99) with
  coordinated-omission correction, so a stalled target shows up as latency, not
  as a hidden throttle.
- **Test types** — `benchmark` (peak), `load` (fixed rate), `stress` (ramp to
  the knee), `acid` (correctness under load), `combined` (chaos under load).
- **Correctness** — reconciliation (loss / corruption / replica convergence),
  Kafka log integrity, and register/list-append checkers.
- **Fault injection** — kill / pause / partition / clock-skew a node mid-run,
  auto-healed, with a recorded timeline correlated to the metrics.
- **Distributed load** — scale out across load machines with a synchronized
  start and lossless histogram merge when one client isn't enough.
- **Results** — self-contained HTML report per run, a DuckDB warehouse for
  cross-run trends, and optional TSDB export for live Grafana.

## Requirements

- Go 1.26+
- Docker (with the Compose plugin) for `--provision` and the cluster bundles
- Optional: `kubectl` for the Kubernetes backend; `elle-cli` for list-append
  isolation checking

## Quick start (PostgreSQL)

```bash
# build all engine binaries into ./bin
./proofload.sh build

# spin up a 3-node Postgres cluster and benchmark peak throughput, then tear it down
export PGPASSWORD=proofload
./proofload.sh postgresql run --test benchmark --workload oltp_read_write --provision compose \
  --warmup 3s --duration 10s --connections 32
```

Output (abridged):

```
proofload · postgresql · oltp_read_write
  test         benchmark
  throughput   23743 req/s
  p50 0.30ms  p95 2.07ms  p99 30.11ms  p99.9 123.80ms  max 557.84ms
  results      results/postgresql/postgresql-<id>
  report       results/postgresql/postgresql-<id>/report.html
```

Fixed-rate **load** test with latency SLAs:

```bash
./proofload.sh postgresql run --test load --rate 15000 --workload oltp_read_write \
  --provision compose --duration 10s --connections 32
```

**Node-failure** test — kill a replica mid-run and verify no data is lost:

```bash
cat > faults.yaml <<'EOF'
faults:
  - {type: kill-node, at: 4s, duration: 5s, target: pg-2}
EOF
./proofload.sh postgresql run --test combined --workload register --provision compose \
  --warmup 3s --duration 15s --verify reconciliation --fault faults.yaml
```

Test an **existing / managed** cluster instead of provisioning one:

```bash
./proofload.sh postgresql run --test load --rate 20000 --workload oltp_read_write \
  --endpoints db1.internal:5432,db2.internal:5432
```

Each system's page has runnable examples for that engine — start with the
[PostgreSQL guide](docs/postgres.md).

## Documentation

- **[Concepts](docs/concepts.md)** — the ideas shared by every target: run
  lifecycle, test types, coordinated omission, correctness models, fault
  injection, distributed load, provisioning, and results/reporting.
- Per-system guides:
  [PostgreSQL](docs/postgres.md) ·
  [Redis](docs/redis.md) ·
  [Cassandra](docs/cassandra.md) ·
  [Kafka](docs/kafka.md) ·
  [ClickHouse](docs/clickhouse.md)

## Command reference

```
./proofload.sh build [target]            # build one or all engine binaries
./proofload.sh list                      # list available targets
./proofload.sh <target> run     [flags]  # run a workload (see --help)
./proofload.sh <target> worker  [flags]  # join a coordinator as a distributed worker
./proofload.sh <target> schema  [flags]  # apply the schema only
./proofload.sh <target> list-workloads   # list a target's workloads
./proofload.sh <target> report  <run-dir># (re)render a run's HTML report
./proofload.sh <target> ingest  <run-dir># load a run into the DuckDB warehouse
./proofload.sh <target> runs             # list warehoused runs, newest first
```

Run `./proofload.sh <target> run --help` for the full flag set (`--test`,
`--rate`, `--connections`, `--warmup`, `--duration`, `--provision`, `--verify`,
`--fault`, `--workers`, `--export`, `--keep`, `--endpoints`, …).
