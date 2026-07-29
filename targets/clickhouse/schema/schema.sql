-- proofload ClickHouse schema.
-- Idempotent: safe to apply repeatedly (provision.sh runs this on every setup).
-- Must stay in sync with createTableSQL in clickhousedriver/sql.go.
--
-- k   : key (Int64; the ORDER BY key, so read/update/scan use its sparse index)
-- v   : opaque value bytes, stored as a ClickHouse String
-- seq : monotonic-per-key sequence; the ReplacingMergeTree version column, so
--       the row with the greatest seq wins and reads pick it explicitly with
--       ORDER BY seq DESC LIMIT 1.
--
-- Clustered (replicated) variant — the active statement. ON CLUSTER 'proofload'
-- fans the DDL out to every replica; {shard}/{replica} come from each server's
-- <macros> (see cluster/config/macros-ch*.xml). Replication is coordinated by
-- clickhouse-keeper, so a killed replica's rows survive on its peer and
-- reconverge on restart. Reads/writes go straight to a server replica — there is
-- no Distributed table.
CREATE TABLE IF NOT EXISTS proofload_kv ON CLUSTER 'proofload'
(
    k   Int64,
    v   String,
    seq Int64
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/proofload_kv', '{replica}', seq)
ORDER BY k;

-- Standalone (single-node) variant — no Keeper, no replication. Use this instead
-- of the statement above when pointing at one ungrouped ClickHouse server:
--
--   CREATE TABLE IF NOT EXISTS proofload_kv
--   (
--       k   Int64,
--       v   String,
--       seq Int64
--   )
--   ENGINE = ReplacingMergeTree(seq)
--   ORDER BY k;
