# ClickHouse

proofload drives ClickHouse as a key/value target over the native protocol, exercising a single shard of two `ReplicatedReplacingMergeTree` server replicas whose replication is coordinated by a three-node `clickhouse-keeper` RAFT ensemble. Because writes are append-only inserts with a per-key `seq` version column, the target measures OLAP-style ingest and read throughput while letting the correctness harness confirm that acknowledged data survives a killed replica and reconverges through Keeper on restart.

## Cluster topology

The bundled `--provision compose` cluster (`targets/clickhouse/infrastructure/docker-compose.yml`) runs five containers on official images pinned to `24.8`: `clickhouse/clickhouse-server:24.8` for the two server replicas and `clickhouse/clickhouse-keeper:24.8` for the three Keeper nodes. It is one logical cluster named `proofload` with **one shard and replication factor 2** (`target.yaml`).

Only the two server replicas (`ch1`, `ch2`) are load endpoints — proofload connects straight to a server replica; there is no `Distributed` table. The three `clickhouse-keeper` nodes are coordination-only: they form the RAFT quorum that `ReplicatedReplacingMergeTree` uses to coordinate replication, and never serve client load.

| Node | Image | Role | Host ports | Fault role |
|------|-------|------|-----------|------------|
| `clickhouse-ch1` | `clickhouse-server:24.8` | server replica (load endpoint) | `19000→9000` (native), `18123→8123` (HTTP) | `replica` |
| `clickhouse-ch2` | `clickhouse-server:24.8` | server replica (load endpoint) | `19001→9000` (native), `18124→8123` (HTTP) | `replica` |
| `clickhouse-keeper1` | `clickhouse-keeper:24.8` | Keeper (RAFT), coordination-only | `9181` (ephemeral) | `seed` |
| `clickhouse-keeper2` | `clickhouse-keeper:24.8` | Keeper (RAFT), coordination-only | `9181` (ephemeral) | `seed` |
| `clickhouse-keeper3` | `clickhouse-keeper:24.8` | Keeper (RAFT), coordination-only | `9181` (ephemeral) | `seed` |

Configuration under `infrastructure/config/` is mounted read-only:

- `keeper1.xml` / `keeper2.xml` / `keeper3.xml` — each Keeper's `<keeper_server>` with its `server_id` and the shared three-server `<raft_configuration>` (RAFT peers on port `9234`, client port `9181`).
- `server-common.xml` — the `<remote_servers>` definition of cluster `proofload` (one shard, two replicas, `internal_replication=true` so a `Replicated*` engine writes once and lets Keeper fan replication out) plus the `<zookeeper>` block pointing both servers at the three Keeper nodes.
- `macros-ch1.xml` / `macros-ch2.xml` — the per-server `{shard}` / `{replica}` macros substituted into the `ReplicatedReplacingMergeTree` znode path and replica name.
- `users.xml` — the `default` user profile.

`proofload-cluster.json` maps each compose service to a proofload node so the compose provisioner can wire them as fault-controllable nodes: `ch1`/`ch2` as `replica` (client port `9000`), `keeper1`–`keeper3` as `seed`.

## Data model & operations

Every operation touches one fixed table, `proofload_kv(k Int64, v String, seq Int64)`, ordered by `k` (`schema/schema.sql`, kept in sync with `createTableSQL` in `clickhousedriver/sql.go`):

- `k` — the key and the `ORDER BY` key, so reads/updates/scans use its sparse primary index.
- `v` — opaque value bytes stored as a ClickHouse `String`.
- `seq` — the monotonic-per-key version column; the row with the greatest `seq` wins.

Clustered runs use `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/proofload_kv', '{replica}', seq)` created `ON CLUSTER 'proofload'`; standalone (single-server) runs use a plain `ReplacingMergeTree(seq)` with the same latest-seq-wins semantics but no Keeper and no replication.

Operations map to SQL in `planFor` (`sql.go`) and are executed in `conn.go`:

- **read** (`read`/`r`) → `SELECT v FROM proofload_kv WHERE k = ? ORDER BY seq DESC LIMIT 1`. Picking the greatest `seq` makes the latest write visible immediately, independent of background merges. A missing key is not an error (nil observed value).
- **insert / update** (`insert`/`w`/`update`) → `INSERT INTO proofload_kv (k, v, seq) VALUES (?, ?, ?)`. ClickHouse has no in-place UPDATE on the hot path, so **updates are append-only inserts of a new `(k, v, seq)` row** — the latest `seq` wins and reads enforce that explicitly.
- **scan** → `SELECT k, v FROM proofload_kv WHERE k >= ? ORDER BY k LIMIT ?`, where the limit comes from `params.scan_limit` / `params.limit` (default `100`).

## Workloads

Run `proofload-clickhouse list-workloads` to enumerate them. All ship in `targets/clickhouse/workloads/`:

| Workload | Mode | Key space | Key dist | Value size | Operation mix |
|----------|------|-----------|----------|-----------|---------------|
| `kv_verify` | performance | 5,000 | uniform | 32 B | insert 80 / read 20 |
| `oltp_read_write` | performance | 1,000,000 | zipfian | 100 B | read 80 / update 15 / insert 5 |
| `write_heavy` | performance | 200,000 | uniform | 64 B | insert 70 / read 30 |

None of the shipped workloads pin a `verify_model`; supply `--verify reconciliation` (or `--test acid`/`combined`) to run a correctness check.

## Consistency

The target exposes two levels (`target.yaml`, `clickhousedriver/config.go`), selected with `--consistency`:

- **none** (default) — leaves ClickHouse session defaults; no quorum requirement.
- **quorum** — applies session settings `insert_quorum=2` and `select_sequential_consistency=1`, so an acknowledged write is durable on both replicas before it can be read back, and a later read is sequentially consistent.

Unknown levels are rejected so misconfiguration fails fast.

## Correctness (verify)

The supported verify model is **reconciliation** (`target.yaml`: `verify_models: [reconciliation]`). After the load phase it confirms that data written during the run survived — including through a killed replica — because `ReplicatedReplacingMergeTree` replicates each row to the peer replica via Keeper, and a killed replica's rows reconverge on restart. Combine it with fault injection (below) to prove replica-loss durability rather than just steady-state presence. See [concepts.md](concepts.md#correctness) for the general model.

## Fault injection

`targets/clickhouse/faults/fault.sh` is the nemesis controller (driven by `core/nemesis.ScriptController`). It supports the fault types advertised in `target.yaml` (`kill-node`, `pause-node`, `network-partition`) plus `clock-skew`, over `docker`, `k8s`, or `ssh`, with a matching `heal` that best-effort reverses every effect.

Expected behavior depends on which node is hit:

- **Killing a server replica** (e.g. `clickhouse-ch2`) — its rows survive on the peer replica, which keeps serving reads and writes; on restart the killed replica reconverges through Keeper. Pair this with `--verify reconciliation` to assert no acknowledged data was lost.
- **Killing a Keeper node** — the RAFT ensemble of three tolerates one down (a 2-of-3 quorum remains), so replication coordination continues.

See [concepts.md](concepts.md#fault-injection) for schedule format and controller details.

## Usage

Real host ports come from the compose bundle: `ch1` native `19000`, `ch2` native `19001`.

```bash
# Benchmark (max throughput) against the bundled 2-replica + 3-keeper cluster
proofload-clickhouse run --workload oltp_read_write --test benchmark \
  --provision compose

# Constant-rate load with quorum durability
proofload-clickhouse run --workload write_heavy --test load \
  --rate 5000 --consistency quorum --provision compose

# Stress: ramp to the knee
proofload-clickhouse run --workload kv_verify --test stress --provision compose

# Fault + reconciliation: kill a server replica mid-run, then verify survival.
# fault.yaml (inline schedule):
#   - at: 10s
#     inject: {type: kill-node, target: clickhouse-ch2}
#     heal_after: 10s
proofload-clickhouse run --workload oltp_read_write --test combined \
  --provision compose --fault fault.yaml --verify reconciliation

# Point at an already-running external cluster (no provisioning)
proofload-clickhouse run --workload write_heavy \
  --endpoints 127.0.0.1:19000,127.0.0.1:19001 --consistency quorum
```

Before an external run, apply the schema and check connectivity with `targets/clickhouse/provision.sh` (honours `PROOFLOAD_CLICKHOUSE_ADDR`, `CLICKHOUSE_USER`, `PROOFLOAD_CLICKHOUSE_PASSWORD`; `--reset` truncates `proofload_kv`), or use `proofload-clickhouse schema --workload <name>`.

## Results

Each run writes a `results/` subtree (base set by `--results`, default `.`) containing `manifest.json`, `summary.json`, `timeseries.ndjson`, `timeseries.parquet`, `latency.hlog`, `verify.json` (when a verify model ran), and a self-contained `report.html`. Render or re-render a report from a run directory with `proofload-clickhouse report`, load a run into the cross-run DuckDB warehouse with `proofload-clickhouse ingest <run-dir>`, and list warehoused runs with `proofload-clickhouse runs`. See [concepts.md](concepts.md#results--reporting).

## Notes & limitations

- **Append-only, latest-seq-wins.** There is no in-place UPDATE; updates are inserts of a higher-`seq` row and reads resolve the winner with `ORDER BY seq DESC LIMIT 1`, so correctness never waits on background merges.
- **Deterministic value bytes.** Values are generated deterministically, so reconciliation checks presence and integrity of the expected bytes rather than reconstructing an arbitrary latest state.
- **OLAP engine.** ClickHouse is column-oriented and optimized for bulk analytics; frequent single-row updates are atypical for the engine, which is exactly why the append-only model is used here.
- **Keeper RAFT tolerance.** The three-node Keeper ensemble tolerates a single failure (2-of-3 quorum); losing two Keepers stalls replication coordination.
- **Single shard.** The bundled topology is one shard with two replicas; there is no sharding or `Distributed` table — load goes directly to a server replica.
