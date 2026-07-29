#!/usr/bin/env bash
# proofload PostgreSQL primary bootstrap (runs once, during initdb, on pg1 only).
#
# The official postgres image executes every /docker-entrypoint-initdb.d/*.sh
# after initdb but before the real server accepts external connections, while a
# local-socket-only bootstrap server is running. We use it to:
#   1. create the physical-replication role `repl` (scram-sha-256 encrypted,
#      since password_encryption defaults to scram-sha-256 on PG16), and
#   2. append pg_hba.conf rules that allow (a) replication connections from the
#      docker subnet and (b) normal TCP logins from the docker subnet.
# The replication GUCs (wal_level, max_wal_senders, ...) are set as `postgres -c`
# command flags in docker-compose.yml, not here.
set -euo pipefail

REPL_USER="${REPL_USER:-repl}"
REPL_PASSWORD="${REPL_PASSWORD:-replpass}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	DO \$\$
	BEGIN
	  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${REPL_USER}') THEN
	    CREATE ROLE ${REPL_USER} WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';
	  END IF;
	END
	\$\$;
SQL

# Persisted in $PGDATA, so these rules survive restarts and are read by the real
# server on startup. scram-sha-256 matches the image's default password_encryption.
cat >> "$PGDATA/pg_hba.conf" <<-HBA

	# --- proofload: streaming replication + TCP client access from docker subnet ---
	host    replication    ${REPL_USER}    0.0.0.0/0    scram-sha-256
	host    all            all             0.0.0.0/0    scram-sha-256
HBA

echo "proofload: primary bootstrap complete (repl role + pg_hba rules installed)."
