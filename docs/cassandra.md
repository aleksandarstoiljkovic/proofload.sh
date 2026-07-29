# Cassandra

proofload drives Apache Cassandra as a distributed, masterless key-value store: a 3-node ring in a single datacenter with replication factor 3, exercised through prepared CQL at a configurable consistency level. Because every node is a peer (no coordinator process), one node can be killed mid-run and — with RF=3 at QUORUM — reads and writes keep succeeding while proofload's reconciliation verifier confirms no data was lost.

## Cluster topology

The target runs the official `cassandra:5` image (`image: cassandra`, `version: "5"` in `target.yaml`). The bundled compose stack (`infrastructure/docker-compose.yml`) brings up a single ring — cluster name `proofload`, datacenter `datacenter1`, rack `rack1` — using the `GossipingPropertyFileSnitch`. Nodes discover each other purely through gossip against the seeds `cass1,cass2`; there is no external coordinator or config store. Bootstrap is staggered on purpose: only one node may join the ring at a time, so `cass2` waits for `cass1` to become healthy and `cass3` waits for `cass2`, where "healthy" means CQL is actually answering a `cqlsh 'describe cluster'` probe.

Each node publishes its native protocol port (9042) on a distinct host port. `infrastructure/proofload-cluster.json` maps each compose service to its client port so the compose provisioner can wire each container as a fault-controllable node (node ID = service name).

| Node ID | Container | Host port → 9042 | Role | Seed |
|---------|-----------|------------------|------|------|
| cass1   | cass-1    | 19042            | node | yes  |
| cass2   | cass-2    | 19043            | node | yes  |
| cass3   | cass-3    | 19044            | node | no   |

Replication factor comes from `cluster.replication_factor` in `target.yaml` (3). The driver's `Schema()` resolves that value and creates the keyspace with it, so the topology is `RF = number of nodes`: every node holds a full copy of the data.

## Data model & operations

The schema (`schema/schema.cql`, kept in sync with `cassandradriver/cql.go`) is a single keyspace `proofload` (SimpleStrategy) and one table:

```cql
CREATE TABLE proofload.kv (
    k   bigint PRIMARY KEY,   -- partition key; point ops are single-partition
    v   blob,                 -- opaque value bytes
    seq bigint                -- monotonic-per-key sequence, used by verifiers
);
```

Cassandra has no engine choice and no distinct upsert — a blind write is last-write-wins — so `insert` and `update` both map to plain writes. Each workload operation resolves to prepared/bound CQL (gocql caches by statement text):

| Op            | CQL |
|---------------|-----|
| `read` / `r`  | `SELECT v FROM kv WHERE k = ?` |
| `insert` / `w`| `INSERT INTO kv (k, v, seq) VALUES (?, ?, ?)` (provides upsert) |
| `update`      | `UPDATE kv SET v = ?, seq = ? WHERE k = ?` |
| `scan`        | `SELECT k, v FROM kv WHERE token(k) >= token(?) LIMIT ?` |

**Token-range scan caveat:** `k` is the partition key, whose on-disk order is by `token(k)` (a hash), not by value. A value range like `k >= ?` is not a valid restriction without `ALLOW FILTERING` (a full-cluster scan), so proofload scans by token range instead. Rows come back in token (hash) order, not key order, and the scan is bounded by `LIMIT` (`params.scan_limit` / `params.limit`, default 100).

## Workloads

Run `./bin/proofload-cassandra list-workloads` to enumerate them. All three ship with `verify_model: ""` (correctness is opt-in via `--verify`).

| Workload | Key space | Key dist | Value | Operation mix |
|----------|-----------|----------|-------|---------------|
| `kv_verify`       | 2,000       | uniform  | 32 B  | insert 80 / read 20 |
| `oltp_read_write` | 10,000,000  | zipfian  | 100 B | read 80 / update 15 / insert 5 |
| `write_heavy`     | 10,000,000  | uniform  | 100 B | insert 60 / update 30 / read 10 |

Run defaults (from `target.yaml`): 64 connections, 60s warmup, 300s duration.

## Consistency

The driver exposes four levels, weakest to strongest: `one`, `quorum`, `local_quorum`, `all`. The `--consistency` flag selects one via gocql; an empty value defaults to **QUORUM**, and an unrecognized level fails fast. The chosen level is set on the cluster config, so every operation on every session runs at that level.

With RF=3, QUORUM means a read or write must reach 2 of the 3 replicas. That is exactly what lets the ring tolerate one node down: 2 replicas remain reachable, so the quorum is still met and operations continue to succeed with no loss of the strong-consistency guarantee. `local_quorum` behaves the same here since there is a single datacenter.

## Correctness (verify)

Pass `--verify reconciliation` to run a correctness check after the load phase. Reconciliation reads each key directly from every individual node and compares replicas to detect divergence and measure staleness. The driver's `ReadKeyFrom` pins a short-lived session to one host (whitelist host filter, initial host lookup disabled) and reads at consistency **ONE**, forcing the answer to come from that node's own replica rather than a quorum of peers — so a genuine replica disagreement is visible rather than masked. See [concepts.md](concepts.md#correctness) for the shared correctness model.

## Fault injection

`faults/fault.sh` is the nemesis's node controller (driven by `core/nemesis.ScriptController`), supporting three control methods — `docker`, `k8s`, `ssh`. Supported fault types: `kill-node` (docker kill / pod delete / SIGKILL), `pause-node` (docker pause / SIGSTOP), `network-partition` (network disconnect / NetworkPolicy / iptables DROP), plus best-effort `clock-skew` and a Cassandra-specific `flush` (`nodetool flush`). `heal` reverses every applied effect best-effort; a killed docker node rejoins the ring on `docker start`. `target.yaml` advertises `faults: [kill-node, pause-node, network-partition]`.

Killing one node under load at QUORUM (RF=3, 2/3 up) is designed to be survivable: the surviving two replicas satisfy the quorum, so reads and writes still succeed and no data is lost. Be honest about the transient behavior, though — the moment a node dies, in-flight queries routed to it fail until the driver marks the host down and reroutes. gocql's `SimpleRetryPolicy{NumRetries: 3}` plus token-aware round-robin host selection retries those on a live coordinator, so you typically see a short error burst whose size varies with timing (which queries were in flight, how fast the host is detected as down), converging back to zero errors with the data intact. See [concepts.md](concepts.md#fault-injection).

## Usage

Host ports are those published by the compose stack (19042/19043/19044). Provisioning is slow: each node takes roughly 60–90s to bootstrap and they start one at a time (staggered), so `--provision compose` can take several minutes before load begins.

```bash
# Max-throughput benchmark, provisioning + tearing down a 3-node ring
./bin/proofload-cassandra run --workload oltp_read_write --provision compose \
  --test benchmark

# Constant-rate load at 5,000 ops/sec
./bin/proofload-cassandra run --workload write_heavy --provision compose \
  --test load --rate 5000

# Stress: ramp until the throughput knee
./bin/proofload-cassandra run --workload oltp_read_write --provision compose \
  --test stress

# Fault + reconciliation: kill one node mid-run at QUORUM, then verify no loss.
# Write the schedule inline, then reference it with --fault:
cat > /tmp/kill-one.yaml <<'YAML'
faults:
  - {type: kill-node, at: 30s, duration: 20s, target: cass2}
YAML
./bin/proofload-cassandra run --workload kv_verify --provision compose \
  --consistency quorum --test combined \
  --fault /tmp/kill-one.yaml --verify reconciliation

# Run against an already-running (external) cluster instead of provisioning.
# Provision its schema first with targets/cassandra/provision.sh.
./bin/proofload-cassandra run --workload oltp_read_write \
  --endpoints localhost:19042,localhost:19043,localhost:19044 \
  --consistency quorum --test benchmark
```

Node control (the `--fault` schedule) requires `--provision` so proofload owns the containers. For an external cluster, provision the schema out of band: `PROOFLOAD_CASSANDRA_HOSTS="10.0.0.1:9042,..." targets/cassandra/provision.sh [--reset]` (optional auth via `PROOFLOAD_CASSANDRA_USER` / `PROOFLOAD_CASSANDRA_PASSWORD`).

## Results

Each run writes a self-contained artifact set under `results/` (base dir set by `--results`): `summary.json`, `history.ndjson`, `timeseries.ndjson`, a run `manifest.json`, `verify.json` (when `--verify` is set), and `faults.json` (the fault timeline, when a schedule ran). Render a browsable report with `./bin/proofload-cassandra report <run-dir>` (produces `report.html`), load a run into the DuckDB warehouse with `./bin/proofload-cassandra ingest <run-dir>`, and list warehoused runs newest-first with `./bin/proofload-cassandra runs`. See [concepts.md](concepts.md#results--reporting).

## Notes & limitations

- **Token-range scans, not key-range.** `scan` walks the ring by `token(k)`; results are in hash order and bounded by `LIMIT`, so it is not an ordered key range. There is no key-ordered scan without `ALLOW FILTERING`.
- **Transient errors on node kill.** A `kill-node` fault produces a brief error burst (size varies by timing) while the driver detects the down host and reroutes; with RF=3 at QUORUM there is no data loss.
- **Replication factor is fixed by the cluster spec.** RF comes from `cluster.replication_factor` in `target.yaml` (3); the schema DDL uses SimpleStrategy, single datacenter.
- **No register / LWT model.** Writes are blind last-write-wins upserts; proofload does not use lightweight transactions (compare-and-set), so the only correctness model wired for this target is `reconciliation`.
- **Slow, staggered bootstrap.** Nodes join the ring one at a time (~60–90s each), so `--provision compose` runs are slow to start.
- **NAT-aware connection.** The driver disables initial host lookup and uses only the provided `host:port` endpoints (it does not read `system.peers`), because a dockerized ring advertises internal container IPs unreachable from the host; each published port is honored per endpoint.
