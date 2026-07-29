package pgdriver

// createTableSQL is the idempotent DDL for the key/value table. It must stay in
// sync with targets/postgresql/schema/schema.sql, which the external
// provisioner applies. The primary key on k provides the index used by read,
// update, upsert (ON CONFLICT), and scan.
const createTableSQL = `CREATE TABLE IF NOT EXISTS proofload_kv (
	k   BIGINT PRIMARY KEY,
	v   BYTEA  NOT NULL,
	seq BIGINT NOT NULL
)`
