-- proofload PostgreSQL schema.
-- Idempotent: safe to apply repeatedly (provision.sh runs this on every setup).
-- Must stay in sync with createTableSQL in pgdriver/schema.go.
--
-- k   : key (BIGINT primary key; its index serves read, update, upsert, scan)
-- v   : opaque value bytes
-- seq : monotonic-per-key sequence, used by correctness verifiers
CREATE TABLE IF NOT EXISTS proofload_kv (
    k   BIGINT PRIMARY KEY,
    v   BYTEA  NOT NULL,
    seq BIGINT NOT NULL
);
