# PostgreSQL

Testing PostgreSQL with proofload drives a real primary + streaming-replica
cluster (or your own external server) under closed- or open-loop load while
recording full latency distributions. On top of raw throughput it verifies
correctness: it reconciles committed writes against what the cluster actually
stores, per-key-linearizability-checks a register workload, and observes
behavior while nodes are killed, paused, or partitioned.

## Cluster topology

The `infrastructure/` bundle provisions one primary and two hot-standby
replicas on the official, unmodified `postgres:16` image. Physical/streaming
replication is set up by proofload itself (no third-party wrapper image): the
primary runs with `wal_level=replica`, `max_wal_senders`, `hot_standby=on`, and
a one-shot initdb hook (`config/primary-init.sh`) that creates a `repl`
replication role and appends the matching `pg_hba` rules; each replica runs
`config/standby-entrypoint.sh`, which `pg_basebackup -R` clones the primary into
an empty `PGDATA` and starts as a hot standby. The primary serves reads and
writes; the replicas serve reads.

`--provision compose` starts this bundle verbatim and reads
`proofload-cluster.json` to wire each container as a fault-controllable node.
Each manifest entry names a compose `service`, its `role`, and the container
port (`client_port: 5432`); the compose provisioner resolves the published host
port via `docker compose port` and the control ref from `docker compose ps`
(the container name). Every service in the manifest carries a client port, so
all three become load endpoints (`127.0.0.1:<host-port>`).

| Service | Role    | Host port | Container |
|---------|---------|-----------|-----------|
| pg1     | primary | 15432     | pg-1      |
| pg2     | replica | 15433     | pg-2      |
| pg3     | replica | 15434     | pg-3      |

All three containers listen on `5432` internally. The provisioned cluster
authenticates as superuser `postgres`, database `proofload`, password
`proofload` (scram-sha-256).

## Data model & operations

One idempotent table (`schema/schema.sql`, kept in sync with `pgdriver`):

```sql
CREATE TABLE IF NOT EXISTS proofload_kv (
    k   BIGINT PRIMARY KEY,   -- key; its index serves read/update/upsert/scan
    v   BYTEA  NOT NULL,      -- opaque value bytes
    seq BIGINT NOT NULL       -- monotonic-per-key sequence, used by verifiers
);
```

`pgdriver` maps each operation type to a prepared statement (`pgdriver/sql.go`).
`read`/`r` and `insert`/`w` are aliases so both performance and correctness
workloads share one plan:

| Op type       | Kind  | SQL |
|---------------|-------|-----|
| `read`, `r`   | read  | `SELECT v FROM proofload_kv WHERE k=$1` |
| `insert`, `w` | write | `INSERT INTO proofload_kv (k,v,seq) VALUES ($1,$2,$3) ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, seq=EXCLUDED.seq` |
| `update`      | write | `UPDATE proofload_kv SET v=$2, seq=$3 WHERE k=$1` |
| `scan`        | scan  | `SELECT k,v FROM proofload_kv WHERE k>=$1 ORDER BY k LIMIT $2` |

The scan limit comes from `params.scan_limit` (or `params.limit`), defaulting to
100. A read that hits no row is not an error (empty result, 0 rows). Each
`Execute` runs exactly one statement inside PostgreSQL's implicit transaction.

## Consistency

`pgdriver` supports three transaction isolation levels (`pgdriver/cluster.go`,
`pgdriver/sql.go`), ordered weakest to strongest:

| `--consistency` value | PostgreSQL isolation |
|-----------------------|----------------------|
| `read-committed` (also the empty default) | READ COMMITTED |
| `repeatable-read`     | REPEATABLE READ |
| `serializable`        | SERIALIZABLE |

The level is applied once per session with
`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`, so every
single-statement implicit transaction runs at that level without an explicit
`BEGIN`/`COMMIT`. Unknown values are rejected so misconfiguration fails fast.
`target.yaml` advertises `read-committed` and `serializable` as the headline
pair; `repeatable-read` is also accepted.

## Correctness (verify)

`target.yaml` declares `verify_models: [reconciliation, register]`. See
[concepts.md#correctness](concepts.md#correctness) for the model definitions.

- **reconciliation** — the default write-verification model. During the run,
  every committed write (key, value checksum, seq) is logged; afterwards the
  checker reads each key back and reports lost or stale values. Because
  `pgdriver` is `ClusterAware`, reconciliation can also read a key directly from
  each replica's own endpoint (`ReadKeyFrom`) to detect replica divergence and
  measure staleness/convergence.
- **register** — the `register.yaml` workload (`mode: correctness`, 50/50
  `r`/`w` over a 1000-key uniform space) records a full operation history and
  runs the Porcupine per-key linearizability checker.

Select a model with `--verify` or let the workload's `verify_model` field drive
it (`oltp_read_write` sets none; `register` sets `register`). Results land in
`verify.json`.

## Fault injection

`faults/fault.sh` implements `kill-node`, `pause-node`, `network-partition`, and
`clock-skew` across the `docker`, `k8s`, and `ssh` control methods; `heal`
reverses every applied effect best-effort. `target.yaml` advertises
`kill-node`, `pause-node`, `network-partition`. For the compose cluster, faults
map to `docker kill` / `docker pause` / `docker network disconnect`, and healing
runs `docker start` / `docker unpause` / `docker network connect`. See
[concepts.md#fault-injection](concepts.md#fault-injection).

Expected behavior, honestly:

- **Kill a replica (pg-2/pg-3)** — the primary and the surviving replica keep
  serving; writes never stop and no data is lost. When healed, the killed
  replica restarts and re-streams from the primary (its `PGDATA` is already
  cloned, so it skips base backup).
- **Kill the primary (pg-1)** — this is a **write outage**. There is no
  automatic failover: replicas stay read-only and writes fail until the primary
  restarts and the replicas reconnect. Committed data is safe, but availability
  for writes is not maintained.

## Usage

Commands use the `proofload.sh <target> <subcommand>` passthrough. Host ports
match the compose bundle; `PGPASSWORD=proofload` matches the provisioned
cluster's superuser password.

```bash
# (a) Benchmark: max-throughput closed-loop run, self-provisioned compose cluster.
PGPASSWORD=proofload ./proofload.sh postgresql run \
  --test benchmark --workload oltp_read_write \
  --provision compose --connections 64 --warmup 60s --duration 300s

# (b) Load test at a fixed arrival rate (open-loop, 20k ops/sec).
PGPASSWORD=proofload ./proofload.sh postgresql run \
  --test load --workload oltp_read_write \
  --provision compose --rate 20000 --duration 300s

# (c) Stress ramp: increase load until the cluster reaches its knee.
PGPASSWORD=proofload ./proofload.sh postgresql run \
  --test stress --workload oltp_read_write \
  --provision compose --connections 128 --duration 180s

# (d) Fault + reconciliation: kill a replica mid-run, then verify no data loss.
cat > kill-replica.yaml <<'YAML'
faults:
  - {type: kill-node, at: 30s, duration: 20s, target: pg-2}
YAML
PGPASSWORD=proofload ./proofload.sh postgresql run \
  --test combined --workload oltp_read_write \
  --provision compose --duration 120s \
  --fault kill-replica.yaml --verify reconciliation

# (e) Point at an EXTERNAL cluster (no provisioning). Credentials come from the
#     environment: PGPASSWORD (or PROOFLOAD_PG_PASSWORD), and optionally
#     PGUSER/PGDATABASE/PGSSLMODE. Default user=postgres, db=proofload,
#     sslmode=disable. Apply the schema first with targets/postgresql/provision.sh.
PGPASSWORD=secret PGUSER=postgres ./proofload.sh postgresql run \
  --test benchmark --workload oltp_read_write \
  --endpoints db1.internal:5432,db2.internal:5432 \
  --consistency serializable --connections 64 --duration 300s
```

`--fault` requires `--provision` (or otherwise-provisioned cluster nodes) for
node control. `--keep` leaves a provisioned cluster running; otherwise it is
torn down (`docker compose down -v`) on exit.

## Results

Each run writes to `results/postgresql/<run-id>/` (run id
`postgresql-YYYYMMDD-HHMMSS-mmm`), under the `--results` base (default `.`):

| Artifact | Contents |
|----------|----------|
| `report.html` | Self-contained HTML report (rendered last, after verify) |
| `summary.json` | Throughput, error count, latency percentiles |
| `manifest.json` | Run config: engine version, seed, connections, client info |
| `timeseries.ndjson` / `timeseries.parquet` | Per-second latency time series |
| `latency.hlog` | HdrHistogram latency log |
| `verify.json` | Correctness verdict (present when a verify model ran) |
| `faults.json` | Fault timeline (present when `--fault` ran) |

Post-run subcommands (see
[concepts.md#results--reporting](concepts.md#results--reporting)):

```bash
./proofload.sh postgresql report results/postgresql/<run-id>   # (re)render report.html
./proofload.sh postgresql ingest results/postgresql/<run-id>   # load into the DuckDB warehouse
./proofload.sh postgresql runs --target postgresql             # list warehoused runs
```

## Notes & limitations

- **No automatic failover.** Killing the primary is a write outage until it
  restarts; proofload does not promote a replica. Plan fault schedules
  accordingly (or target replicas to test read availability).
- **Replicas are read-only.** Writes routed to a standby fail; `pgdriver`
  connects to the first endpoint for the load loop, so put the primary first
  when passing `--endpoints`.
- **Passwords come only from the environment.** `PROOFLOAD_PG_PASSWORD` then
  `PGPASSWORD` — never from checked-in config. The provisioned cluster uses
  `proofload`.
- **Connection defaults.** `user=postgres`, `dbname=proofload`,
  `sslmode=disable`, port `5432`; override via `PGUSER`/`PGDATABASE`/`PGSSLMODE`
  or the endpoint. An endpoint that is already a full DSN (URL or keyword form)
  is passed through unchanged.
- **Each Execute is one statement in an implicit transaction** — isolation is
  set at the session level, so multi-statement transactional anomalies are out
  of scope for this target's workloads.
- **External clusters must be provisioned first.** `targets/postgresql/provision.sh`
  (`PROOFLOAD_PG_DSN=... ./provision.sh [--reset]`) applies the schema and,
  with `--reset`, truncates `proofload_kv`.
- `clock-skew` exists in `fault.sh` but is best-effort (needs a writable clock)
  and is not advertised in `target.yaml`.

See also the top-level [README](../README.md).
