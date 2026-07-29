# Kafka

The `proofload-kafka` engine drives Apache Kafka as a load and correctness target using the franz-go (`kgo`) client. Kafka is a distributed append-only **log broker**, not a keyed KV store, so proofload models operations as *roles* on a shared client — **produce** (writers) and **consume** (readers) — rather than as point reads and writes.

## Cluster topology

The bundled cluster runs the **official Apache image `apache/kafka:3.9.2`** (declared in `target.yaml` and `infrastructure/docker-compose.yml`) as **three brokers in KRaft mode**. Each node runs the combined `broker,controller` process role and together they form the KRaft controller quorum (`KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093`) — the coordination analog to ClickHouse Keeper, with no separate ZooKeeper/controller tier.

Every broker exposes three listeners:

- **CONTROLLER** (`:9093`) — the KRaft quorum, PLAINTEXT, never advertised.
- **INTERNAL** (`:9092`) — inter-broker traffic and in-container clients, advertised as `kafkaN:9092` (Docker-network DNS = the compose service name); named as the inter-broker listener.
- **EXTERNAL** (`:9094`) — host clients, advertised as `127.0.0.1:1909x` and published to that host port.

The host-facing driver seeds off `127.0.0.1:19092` and follows advertised listeners to the other brokers, so **each broker must advertise its own host port** on EXTERNAL — otherwise a host client cannot follow a partition leader off the seed node. Topics are created with **replication factor 3** and `min.insync.replicas=2` (`KAFKA_DEFAULT_REPLICATION_FACTOR=3`, `KAFKA_MIN_INSYNC_REPLICAS=2`).

`infrastructure/proofload-cluster.json` maps each compose service to a fault-controllable node (`role: broker`, `client_port: 9094`); proofload wires the bundle via `--provision compose`.

| Node | Container | Role | INTERNAL advertised | EXTERNAL advertised | Host port |
|------|-----------|------|---------------------|---------------------|-----------|
| kafka1 | kafka-1 | broker + controller | `kafka1:9092` | `127.0.0.1:19092` | 19092 → 9094 |
| kafka2 | kafka-2 | broker + controller | `kafka2:9092` | `127.0.0.1:19093` | 19093 → 9094 |
| kafka3 | kafka-3 | broker + controller | `kafka3:9092` | `127.0.0.1:19094` | 19094 → 9094 |

## Data model & operations

The unit of storage is a **topic** (default `proofload`), partitioned (workloads use 12 partitions) and replicated across the three brokers. A produce record carries the decimal ASCII of `op.Key` as its Kafka key — so records for one logical key route to a single partition, preserving per-key order — with `op.Value` as the payload and the per-key sequence number in a `pl-seq` record header.

Operations map to log roles (verbose and single-letter aliases both accepted):

- **`produce` / `w`** — synchronously produce one record via `ProduceSync`, so the runner times the real end-of-ack latency.
- **`produce_batch` / `wb`** — **batched**: a single `Execute` emits `params.batch_size` records in **one** `ProduceSync` call. franz-go coalesces them into produce requests, so the broker round-trip is amortised over the whole batch and throughput reflects Kafka's true batched ingest rather than the one-record-per-RTT rate. Each record gets a distinct deterministic key (`op.Key*batch_size + i`) so keys fan out without colliding while every derived key still sees a contiguous per-key sequence. `OpResult.Rows` is set to `batch_size` so the summary counts messages, not batches. A plain `produce` also honours `batch_size`, so batching needs no op-label change.
- **`consume` / `r`** — poll exactly one record from a maintained consumer and return its value; an empty poll is not an error.

**`ReadKeyFrom` is unsupported.** Kafka is an append-only log with no per-key point read from a specific broker, so the `ClusterAware.ReadKeyFrom` capability returns an explicit unsupported error and correctness relies on the `kafka-log` verify model instead.

## Workloads

Run `proofload-kafka list-workloads` to enumerate them. All use `value_size: 512`, `key_space: 1000000`, uniform key distribution, and topic `proofload`.

| Workload | Operations | Partitions | batch_size | verify_model | Notes |
|----------|-----------|-----------|-----------|--------------|-------|
| `produce_throughput` | `produce` 100% | 12 | (default 1) | — | Single-record produce; raw write throughput. |
| `produce_consume` | `produce` 50% / `consume` 50% | 12 | (default 1) | `kafka-log` | Mixed read/write; consumer group `proofload-consume`. |
| `batched_produce` | `produce_batch` 100% | 12 | **500** | — | One `Execute` = 500 records in one `ProduceSync`. |

`target.yaml` defaults: `connections: 32`, `warmup: 30s`, `duration: 180s`.

## Consistency

Delivery guarantee is the Kafka `acks` level, parsed from `--consistency` (weakest → strongest): **`acks=0`** (fire-and-forget), **`acks=1`** (leader only), **`acks=all`** (all in-sync replicas). The empty string **defaults to `acks=all`** — the strongest level — and unknown levels are rejected so misconfiguration fails fast. `target.yaml` advertises `consistency: [acks=all, acks=1]`.

**Exactly-once semantics (EOS)** toggles live in workload/target `params`:

- `idempotent` — the idempotent producer; only valid at `acks=all`, where it defaults **on** (a workload can force it off to measure the non-idempotent hot path). Any acks level weaker than `all` disables idempotence automatically.
- `transactional_id` (alias `txn_id`) — enables transactional writes; each produce (or whole batch) is wrapped in a committed transaction. Setting it forces `acks=all` + idempotent so EOS can never be combined with a weaker level.

## Correctness (verify)

Kafka uses the driver-native **`kafka-log`** model (the only entry in `verify_models`), not the reconciliation or register models used by KV targets — those do **not** apply to a log. After the load phase the verifier consumes the whole topic from the start and analyses the per-key `pl-seq` sequence stream:

- **Message loss** — for each key the produced sequences are treated as a contiguous range `[min,max]`; any missing value is a lost message.
- **Duplication** — any repeated sequence for a key.
- **Ordering** — checked per partition in offset order; because a key routes to a single partition, a sequence that decreases relative to the previous record for the same key is an ordering violation.

The verdict is `pass` when loss, duplication, and ordering violations are all zero (`unknown` if the topic drained empty). The analysis core (`analyzeLog`) is a pure function, unit-tested without a broker. See [concepts.md#correctness](concepts.md#correctness).

## Fault injection

`faults/fault.sh` is driven by the proofload nemesis (`ScriptController`) and supports `inject`/`heal` over `--method k8s|docker|ssh`. Implemented fault primitives: **`kill-node`** (`docker kill` / `kubectl delete pod --force` / `pkill -9`), `pause-node` (SIGSTOP), `network-partition` (network disconnect / NetworkPolicy / iptables DROP), and `clock-skew`. `heal` reverses every applied effect best-effort. Note: `target.yaml` also lists `leader-election` and `isr-shrink` under `faults`, but these are **recognised stubs** in `fault.sh` that exit non-zero (unsupported) so a schedule referencing them fails loudly.

Killing a broker mid-produce is safe by design: with **`acks=all` + RF 3 + `min.insync.replicas=2`**, every partition still has a surviving in-sync replica, so a new leader is elected and `acks=all` writes continue through the surviving ISR. See [concepts.md#fault-injection](concepts.md#fault-injection).

## Usage

```bash
# Batched-produce benchmark: spin up the 3-broker KRaft cluster via compose,
# tear it down afterwards. The summary's records line = ops/s × batch_size (500).
./bin/proofload-kafka run --workload batched_produce --provision compose

# Mixed produce/consume with kafka-log correctness verification.
./bin/proofload-kafka run --workload produce_consume --provision compose \
  --verify kafka-log

# Fault run: kill a broker mid-produce and verify no loss/dup/reordering.
./bin/proofload-kafka run --workload produce_consume --provision compose \
  --test combined --fault faults/schedule.yaml --verify kafka-log

# Against an already-running external cluster (host ports 19092-19094),
# no provisioning.
./bin/proofload-kafka run --workload produce_throughput \
  --endpoints 127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094 \
  --consistency acks=all
```

For an external cluster, `provision.sh` (reads `PROOFLOAD_KAFKA_BROKERS`, `PROOFLOAD_KAFKA_TOPIC`, `PROOFLOAD_KAFKA_PARTITIONS`, `PROOFLOAD_KAFKA_REPLICATION`) checks connectivity and creates the topic; `--reset` deletes and recreates it (destructive, opt-in).

## Results

Each run writes an artifact directory under `results/kafka/<runID>/`: `manifest.json`, `summary.json`, `timeseries.ndjson` + `timeseries.parquet`, `latency.hlog`, `verify.json` (when a verify model ran), and `server_stats.json`. `proofload-kafka report <dir>` renders a self-contained HTML report; `ingest` loads a run into the DuckDB warehouse and `runs` lists recorded runs for cross-run analytics.

For batched produce the console summary adds a dedicated records line whenever `Records > Total`, e.g. `records  <n> (<r> rec/s — batched)`. Because one `Execute` is counted as one op, **`throughput` is in ops/s (batches/s)** and the **record rate is `records/s = ops/s × batch_size`** — with `batch_size=500` this commonly reaches ~1M+ msgs/s, versus the ~11k msgs/s of one-record-per-op produce. See [concepts.md#results--reporting](concepts.md#results--reporting).

## Notes & limitations

- **Advertised-listener requirement**: host access works only because each broker advertises its own `127.0.0.1:1909x` EXTERNAL port. A misconfigured advertised listener leaves the seed reachable but the driver unable to follow leaders to the other brokers. Verify with `docker compose -f docker-compose.yml config`.
- **Batched-throughput accounting**: `throughput` is batches/s (ops/s); real message rate is `ops/s × batch_size`. Use the `records`/`rec/s` summary line, not the `req/s` line, when comparing batched ingest.
- **No per-key point read**: `ReadKeyFrom` is unsupported by design; divergence/staleness probes do not apply to a log. Correctness comes from the `kafka-log` verifier only.
- **EOS toggles**: `idempotent` is valid only at `acks=all`; `transactional_id` forces `acks=all` + idempotent. Weaker acks levels disable idempotence automatically.
- **Fault stubs**: `leader-election` and `isr-shrink` are advertised in `target.yaml` capabilities but not yet implemented in `fault.sh` (they exit unsupported).
