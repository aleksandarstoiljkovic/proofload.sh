# proofload concepts

This page explains the ideas shared by every target. Each per-system page
([postgres](postgres.md), [redis](redis.md), [cassandra](cassandra.md),
[kafka](kafka.md), [clickhouse](clickhouse.md)) links back here for the details.

- [Mental model](#mental-model)
- [Run lifecycle](#run-lifecycle)
- [Test types](#test-types)
- [Rate control & coordinated omission](#rate-control--coordinated-omission)
- [Client-bound runs](#client-bound-runs)
- [Correctness](#correctness)
- [Fault injection](#fault-injection)
- [Distributed load](#distributed-load)
- [Provisioning](#provisioning)
- [Results & reporting](#results--reporting)
- [Adding a new target](#adding-a-new-target)

---

## Mental model

proofload is a single orchestrator, `proofload.sh`, that dispatches to one
self-contained Go engine per target (`bin/proofload-<target>`). Every engine
shares the same core: a coordinated-omission-correct load loop, HdrHistogram
metrics, pluggable correctness checkers, fault injection, distributed
coordination, and result storage/reporting. A target only implements *how to
talk to its system* (connect, run an operation, apply schema, read a key back);
everything else is inherited.

```
proofload.sh  ──►  bin/proofload-<target>
                        │
   core/  ── config · workload gen · schedule · runner · metrics ──┐
                        │                                           │
   targets/<t>/  driver (+ClusterAware/Verifier) · workloads · infrastructure/ · faults/
                        │
   verify (reconciliation · register · list-append · kafka-log)
   nemesis (fault injection)   cluster (distributed workers)
   storage: HdrHistogram .hlog · Parquet · DuckDB warehouse · HTML report · TSDB export
```

## Run lifecycle

```
resolve config → provision (optional) → apply schema
   → warmup (discarded) → measure  ⟂ nemesis faults  ⟂ correctness recording
   → collect → verify (optional) → write artifacts + report → teardown (unless --keep)
```

Only the **measure** phase is recorded. The warmup phase runs the same load but
is thrown away so JIT/cache/compaction effects don't skew the numbers.

## Test types

`--test` selects what aspect of the system you're examining. It sets the rate
profile and guardrails; `--verify` and `--fault` compose on top of any type.

| `--test` | Rate profile | Answers |
|---|---|---|
| `benchmark` | closed-loop, saturate | *What's the peak sustainable throughput and its latency?* |
| `load` | constant open-loop at `--rate` | *What are the latency percentiles at a production-like rate?* |
| `stress` | ramp up in 5 steps to `--rate` | *Where does it break?* — the per-second series shows the knee |
| `acid` | controlled load + `--verify` | *Is it correct under load?* |
| `combined` | load + `--fault` + `--verify` | *How does it behave under load with failures?* |

Empty `--test` defaults to `load` when `--rate` is set, otherwise `benchmark`.
`load` and `stress` require `--rate` (for stress it is the ramp ceiling).

## Rate control & coordinated omission

Two loop modes, chosen by the test type:

- **Closed-loop** (`benchmark`): each connection fires the next operation as
  soon as the previous completes — the client pushes as hard as it can. Latency
  is measured from dispatch (true service time). This finds peak throughput.
- **Open-loop** (`load`, `stress`): operations have a fixed *intended* start
  time (`start + n/λ`). Latency is measured from that **intended** time, not
  from when the request was actually sent. This is the **coordinated-omission
  correction**: if the target stalls, the backlog shows up as growing latency
  instead of being hidden by a silently throttled request rate. `stress` uses a
  stepped ramp of intended rates.

## Client-bound runs

A run is flagged **client-bound** only when the *load generator* — not the
target — was the limiter: dispatch fell behind the intended schedule
(`meanLateness > mean inter-arrival`) **and** that backlog isn't explained by
the target being slow (`meanLateness > mean service time`) **and** the error
rate is low (< 5%). In that case the target numbers understate what the system
can do, and the fix is to give the client more capacity: raise `--connections`,
or scale out horizontally with [distributed workers](#distributed-load).

When the *target* is slow (high service latency) or a fault caused errors, the
run is **not** flagged client-bound — coordinated omission already surfaces that
honestly as latency. (This is deliberately conservative to avoid the false
"client-bound" signal that a slow target or an injected fault would otherwise
trigger.)

## Correctness

`--verify <model>` runs a correctness check after the load phase and writes
`verify.json` (verdict + anomalies). Models:

- **`reconciliation`** — during the run every committed write is recorded
  (key + FNV-1a checksum + seq); afterwards every expected key is read back and
  compared. Detects data **loss**, **corruption**, and — for multi-node targets
  — **replica divergence / staleness** (each replica is read directly and polled
  until it converges). Works for any key/value target.
- **`register`** — per-key linearizability via
  [Porcupine](https://github.com/anishathalye/porcupine).
- **`list-append`** — transactional isolation via
  [Elle](https://github.com/jepsen-io/elle) (`elle-cli` on `PATH`; reports
  `unknown` gracefully when it isn't installed).
- **`kafka-log`** — Kafka-native: message loss, duplication, and per-partition
  ordering from per-key sequence headers.

> **Honest limitation.** `register` and `list-append` checkers are correct and
> unit-proven on synthetic histories, but the built-in generator writes a
> deterministic value per key, so *live* histories aren't yet distinguishable
> enough for meaningful anomaly detection — that needs a versioned workload
> (unique write tokens), which is future work. `reconciliation` and `kafka-log`
> are meaningful live today.

## Fault injection

`--fault <schedule.yaml>` runs a nemesis alongside the measure phase and writes
a `faults.json` timeline. It requires a provisioned cluster (`--provision`) so
proofload has a control endpoint (docker/k8s) for each node. Schedule:

```yaml
faults:
  - {type: kill-node, at: 15s, duration: 8s, target: pg-2}
  - {type: network-partition, at: 30s, duration: 5s}   # target omitted = seeded random node
```

Fault types: `kill-node`, `pause-node`, `network-partition`, `clock-skew`
(plus target-specific ones). `at`/`duration`/`repeat` are relative to the
measure start. Every injected fault is **always healed** when the run ends
(even on Ctrl-C), and a killed node is restarted so it can rejoin. Node names
come from each target's `infrastructure/proofload-cluster.json`.

## Distributed load

When one client can't saturate the target, scale out. One process coordinates:

```bash
# coordinator (waits for N workers, merges their results losslessly)
./proofload.sh postgresql run --workload oltp_read_write --endpoints ... --workers 3
# on each load machine
./proofload.sh postgresql worker --coordinator <host:8677>
```

All workers start the measure phase together (a synchronized *start-gun*), each
drives a disjoint shard of the keyspace, and their HdrHistograms are merged
losslessly into one aggregate result.

## Provisioning

`--provision` brings a cluster up so a run (and faults) have real nodes:

- **`compose`** — if the target ships an `infrastructure/` bundle
  (`docker-compose.yml` + `proofload-cluster.json`), proofload runs it verbatim
  and wires every container as a fault-controllable node; otherwise it renders a
  generic N-node topology. This is the tested path for the bundled clusters.
- **`kubernetes`** — renders a StatefulSet per target via `kubectl` (assumes
  in-cluster DNS).
- **external** (no `--provision`) — pass `--endpoints host:port,...` to test an
  existing/managed cluster; proofload only connects (no fault control).

Provisioned clusters are torn down after the run unless you pass `--keep`.

## Results & reporting

Each run writes `results/<target>/<run-id>/`:

| File | Contents |
|---|---|
| `manifest.json` | full reproducibility record (target, versions, params, cluster, client, seed) |
| `summary.json` | aggregate + per-operation percentiles |
| `latency.hlog` | canonical HdrHistogram interval log (every percentile is re-derivable) |
| `timeseries.parquet` / `.ndjson` | per-second throughput + latency + errors |
| `verify.json` | correctness verdict + anomalies (when `--verify`) |
| `faults.json` | nemesis timeline (when `--fault`) |
| `report.html` | self-contained HTML report (inline SVG charts), auto-generated |

Cross-run analytics use an embedded DuckDB warehouse:

```bash
./proofload.sh postgresql ingest results/postgresql/<run-id>
./proofload.sh postgresql runs                 # list recorded runs, newest first
./proofload.sh postgresql report <run-id-dir>  # (re)render report.html
```

Optionally stream metrics to a TSDB for live Grafana dashboards with
`--export influx=<url>` or `--export pushgateway=<url>`.

## Adding a new target

Implement `driver.Driver` (+ optional `ClusterAware` / `Verifier` /
`FaultController`) in `targets/<name>/`, drop in a `target.yaml`, one or more
`workloads/*.yaml`, an optional `infrastructure/` cluster bundle, and a
`faults/fault.sh`. The engine inherits metrics, scheduling, test types,
verification, fault injection, distribution, storage, and reporting. See any
existing target (e.g. [clickhouse](clickhouse.md)) as a template.
