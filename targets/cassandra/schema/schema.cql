-- proofload Cassandra schema.
-- Idempotent: safe to apply repeatedly (provision.sh runs this on every setup).
-- Must stay in sync with createKeyspaceCQL/createTableCQL in cassandradriver/cql.go.
--
-- Keyspace replication_factor here is a provisioning default; the driver's
-- Schema() creates the keyspace with the replication factor resolved from the
-- cluster spec (targets/cassandra/target.yaml -> cluster.replication_factor).
--
-- kv columns:
--   k   : partition key (bigint); reads/writes are single-partition point ops
--   v   : opaque value bytes (blob)
--   seq : monotonic-per-key sequence, used by correctness verifiers
CREATE KEYSPACE IF NOT EXISTS proofload
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS proofload.kv (
    k   bigint PRIMARY KEY,
    v   blob,
    seq bigint
);
