# Redis

The Redis target drives load and correctness checks against a Redis 7 primary/replica cluster fronted by Redis Sentinel, using the `go-redis/v9` client. It models a schemaless string key/value store, maps proofload operations onto `GET`/`SET`/`MGET`, and offers a tunable durability floor via `WAIT`.

## Cluster topology

The shipped Compose bundle (`targets/redis/infrastructure/docker-compose.yml`) runs the official `redis:7` image as **1 primary + 2 replicas** (asynchronous replication) coordinated by **3 Redis Sentinels** with a monitor quorum of 2/3. The data nodes (`redis1`, `redis2`, `redis3`) listen on client port `6379` and are the load endpoints; the Sentinels listen on `26379`, are coordination-only (no client port), and are excluded from load and convergence reads — but remain fault-targetable. `provision.sh`'s companion `proofload-cluster.json` records the service→role→endpoint mapping the compose provisioner uses to wire each container as a fault-controllable node. Auth and protected-mode are disabled in this ephemeral test bundle, so the driver connects with no password on DB 0.

Replicas set `replicaof redis1 6379`; Sentinels monitor master `proofload` with `down-after-milliseconds 5000` and `failover-timeout 10000`.

| Service | Role | Host port | Container name |
|---------|------|-----------|----------------|
| `redis1` | primary (load endpoint) | `16379` → 6379 | `redis-1` |
| `redis2` | replica (load endpoint) | `16380` → 6379 | `redis-2` |
| `redis3` | replica (load endpoint) | `16381` → 6379 | `redis-3` |
| `sentinel1` | Sentinel (coordination-only) | — (26379 internal) | `redis-sentinel-1` |
| `sentinel2` | Sentinel (coordination-only) | — (26379 internal) | `redis-sentinel-2` |
| `sentinel3` | Sentinel (coordination-only) | — (26379 internal) | `redis-sentinel-3` |

In `proofload-cluster.json` the three Sentinels carry role `seed` and no `client_port`, so proofload treats them as coordination nodes.

## Data model & operations

Keys are strings formatted as `proofload:{<k>}` (`keyName` in `redisdriver/cmd.go`). The braces are a Redis Cluster hash tag: in cluster mode keys sharing a tag map to the same slot; in standalone mode they are inert. Each proofload operation maps to a single Redis command:

| Op type | Redis command | Notes |
|---------|---------------|-------|
| `read` / `r` | `GET` | value bytes returned in `Observed`; a missing key is not an error (`Rows = 0`). |
| `insert` / `w` | `SET` | writes `op.Value`; `Rows = 1`. |
| `update` | `SET` | same path as insert (Redis `SET` is an upsert). |
| `scan` | `MGET` | reads a contiguous range of `scan_limit` keys `[start, start+N)`; missing members are skipped so `Rows` reflects keys that existed. |

`scan_limit` (alias `limit`) comes from the workload `params` and defaults to `100`.

## Workloads

Run `./bin/proofload-redis list-workloads` to enumerate them. All ship with `mode: performance` and no verify model set (pass `--verify` to add one).

| Name | Mode | Key dist | Op mix | Purpose |
|------|------|----------|--------|---------|
| `kv_verify` | performance | uniform (key_space 2000, value 32B) | insert 80 / read 20 | Small, dense keyspace tuned for correctness checks (high key reuse). |
| `kv_write` | performance | uniform (key_space 1M, value 100B) | insert 70 / update 25 / read 5 | Write-heavy ingest / durability stress. |
| `oltp_read_write` | performance | zipfian (key_space 1M, value 100B) | read 80 / update 15 / insert 5 | Read-dominant OLTP mix with skewed hot keys. |

## Consistency

The driver exposes two levels (`redisdriver/cluster.go`), weakest to strongest:

- **`none`** (also the empty default) — fire-and-forget. Redis replication is asynchronous, so writes return once the primary accepts them; no extra round-trips.
- **`wait`** (alias `waitN`) — issues a `WAIT <replicas> <timeout>` after each write, blocking until the requested number of replicas acknowledge or the timeout elapses. Defaults are `wait_replicas = 1` (alias `numreplicas`) and `wait_timeout_ms = 100`, both tunable via `params`. `WAIT`'s own timeout bounds the round-trip, so a lagging replica surfaces as a durability shortfall in the results rather than a hang.

Unknown consistency values are rejected so misconfiguration fails fast.

## Correctness (verify)

Redis supports the **`reconciliation`** verify model. After the load phase, the verifier reads keys directly from each queryable node via the driver's `ReadKeyFrom` (a short-lived per-node client that bypasses normal routing) and reconciles the observed values across nodes to detect replica divergence and measure staleness. Because replication is asynchronous, transient divergence during load is expected; reconciliation quantifies how far replicas trail the primary. Only nodes with a client endpoint are read — Sentinels are filtered out by `Nodes()` (see below). See [concepts.md#correctness](concepts.md#correctness).

## Fault injection

`targets/redis/faults/fault.sh` is driven by the nemesis `ScriptController` and supports these fault types across the `docker`, `k8s`, and `ssh` control methods: `kill-node`, `pause-node`, `network-partition` (Compose needs `--param network=<compose network>`, e.g. `proofload-redis_default`), and `clock-skew`. `heal` reverses every applied effect best-effort — for Docker it runs `docker unpause`, `docker start`, and `docker network connect`.

- **Killing a replica (`redis2`/`redis3`)** — the primary and the peer replica keep serving; the killed node rejoins and re-syncs on heal (`docker start`). The cluster survives.
- **Killing the primary (`redis1`)** — Sentinel promotes a replica after `down-after-milliseconds` (~5s), but the load driver targets a **fixed** endpoint (`redis1`) and does not follow the failover, so load against the killed primary surfaces errors until `redis1` is healed. **Prefer killing a replica** for the node-survival demo.

Note that `Nodes()` excludes Sentinels from convergence/staleness reads, but the nemesis can still target them for fault injection via the resolved cluster spec. See [concepts.md#fault-injection](concepts.md#fault-injection).

## Usage

All commands assume the built `./bin/proofload-redis`. Provisioned runs use `--provision compose` (the bundle above); external runs use `--endpoints`.

```bash
# Benchmark: max-throughput closed loop against a provisioned cluster
./bin/proofload-redis run --workload oltp_read_write --provision compose \
  --test benchmark --duration 300s --connections 64

# Load: hold a constant arrival rate
./bin/proofload-redis run --workload kv_write --provision compose \
  --test load --rate 20000 --duration 120s

# Stress: ramp connections/rate to find the knee
./bin/proofload-redis run --workload oltp_read_write --provision compose \
  --test stress --duration 180s

# Fault + reconciliation: kill a replica mid-run, then verify convergence.
# The inline schedule targets redis2 (a replica) so the cluster survives.
cat > /tmp/redis-kill-replica.yaml <<'YAML'
faults:
  - {type: kill-node, target: redis2, at: 20s, duration: 15s}
YAML
./bin/proofload-redis run --workload kv_verify --provision compose \
  --test combined --duration 60s \
  --fault /tmp/redis-kill-replica.yaml --verify reconciliation

# Durability floor: block on one replica ack per write
./bin/proofload-redis run --workload kv_write --provision compose \
  --consistency wait --duration 120s

# External cluster: point at the bundle's published host ports (no provisioning)
./bin/proofload-redis run --workload oltp_read_write \
  --endpoints 127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381 \
  --test benchmark --duration 120s
```

For an external Redis, prepare it first with `PROOFLOAD_REDIS_ADDR=host:port ./targets/redis/provision.sh [--reset]` (connectivity check; `--reset` flushes DB 0). Set `PROOFLOAD_REDIS_PASSWORD` (or `REDIS_PASSWORD`) if the server requires auth.

## Results

Each run writes an artifact directory under `results/redis/<run-id>/` (run-id format `redis-<YYYYMMDD-HHMMSS>-<ms>`), containing `manifest.json`, `summary.json`, `timeseries.ndjson`, `timeseries.parquet`, `latency.hlog`, `server_stats.json`, plus `verify.json` when a verify model ran and `faults.json` when faults were injected.

- `report <run-dir>` renders a self-contained HTML report for a run directory.
- `ingest <run-dir>` loads a run into the DuckDB warehouse (`warehouse/proofload.duckdb`) for cross-run analytics.
- `runs` lists runs recorded in the warehouse, most recent first (filter with `--target`, `--workload`, `--limit`).

See [concepts.md#results--reporting](concepts.md#results--reporting).

## Notes & limitations

- **Asynchronous replication.** By default writes are not waited on; use `--consistency wait` for a durability floor. Expect transient replica staleness under load, which reconciliation quantifies.
- **Fixed endpoint, no failover follow.** The driver opens one connection to the first endpoint and does not follow Sentinel failover, so killing the primary disrupts load until it heals. Prefer killing a replica.
- **Sentinels are coordination-only.** They carry no client endpoint, are excluded from convergence/staleness reads via the driver's `Nodes()` filter, but remain fault-targetable.
- **No auth in the test bundle.** The shipped Compose cluster disables auth and protected-mode and has no persistence (`save ""`, `appendonly no`) — it is ephemeral. External runs can supply a password via `PROOFLOAD_REDIS_PASSWORD`/`REDIS_PASSWORD`, kept off the command line.
- **One connection per session.** Each runner goroutine gets a dedicated `PoolSize 1` client so the runner, not go-redis, controls concurrency (`--connections`).
- **`scan` is an `MGET`, not `SCAN`.** It reads a fixed contiguous key range of `scan_limit` keys rather than iterating the keyspace cursor.
</content>
</invoke>
