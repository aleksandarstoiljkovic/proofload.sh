#!/usr/bin/env bash
# proofload PostgreSQL standby bootstrap (pg2 / pg3).
#
# This runs INSTEAD of the official image's initdb entrypoint (compose sets it as
# the container `command:`, and because argv[0] is not `postgres` the image
# entrypoint execs it directly without initializing a fresh database). It:
#   1. writes a .pgpass so the standby can (re)authenticate to the primary on
#      every start,
#   2. if PGDATA is empty, waits for the primary and clones it with
#      pg_basebackup -R (which writes standby.signal + primary_conninfo), and
#   3. starts postgres as a hot standby (hot_standby=on => serves read queries).
# On a heal/restart PGDATA is already populated, so we skip the clone and just
# resume streaming from primary_conninfo.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${PRIMARY_HOST:-pg1}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPL_USER="${REPL_USER:-repl}"
REPL_PASSWORD="${REPL_PASSWORD:-replpass}"
PGPASSFILE=/var/lib/postgresql/.pgpass
export PGPASSFILE

# Credentials for both the base backup and every subsequent reconnect. Written
# fresh each start; primary_conninfo (below, via -R) references this passfile.
printf '%s:%s:replication:%s:%s\n' "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPL_USER" "$REPL_PASSWORD" >  "$PGPASSFILE"
printf '%s:%s:*:%s:%s\n'           "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPL_USER" "$REPL_PASSWORD" >> "$PGPASSFILE"
chown postgres:postgres "$PGPASSFILE"
chmod 0600 "$PGPASSFILE"

mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
	echo "standby: PGDATA empty; waiting for primary ${PRIMARY_HOST}:${PRIMARY_PORT} ..."
	until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U postgres >/dev/null 2>&1; do
		echo "standby: primary not ready yet, retrying in 2s"
		sleep 2
	done
	echo "standby: cloning primary with pg_basebackup ..."
	gosu postgres pg_basebackup \
		--pgdata="$PGDATA" \
		--wal-method=stream \
		--write-recovery-conf \
		--progress --verbose \
		--dbname="host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=${REPL_USER} passfile=${PGPASSFILE}"
	echo "standby: base backup complete."
fi

chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

echo "standby: starting postgres as hot standby."
exec gosu postgres postgres -c hot_standby=on -c listen_addresses='*'
