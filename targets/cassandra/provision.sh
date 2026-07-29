#!/usr/bin/env bash
#
# provision.sh — prepare an EXTERNAL Cassandra cluster for a proofload run.
#
# Steps:
#   1. verify connectivity to the first contact point in $PROOFLOAD_CASSANDRA_HOSTS
#      with a cqlsh "SELECT now()" probe
#   2. apply schema/schema.cql (idempotent CREATE KEYSPACE / TABLE IF NOT EXISTS)
#   3. optionally TRUNCATE proofload.kv, but ONLY when --reset is passed, so a
#      dirty table is never silently clobbered.
#
# Usage:
#   PROOFLOAD_CASSANDRA_HOSTS="10.0.0.1:9042,10.0.0.2:9042" ./provision.sh [--reset]
#
# Auth (optional) is read from the environment so credentials never live in
# checked-in config:
#   PROOFLOAD_CASSANDRA_USER, PROOFLOAD_CASSANDRA_PASSWORD
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: PROOFLOAD_CASSANDRA_HOSTS=<host:port[,host:port...]> ./provision.sh [--reset]

  --reset   TRUNCATE proofload.kv after applying the schema (destructive).
            Omit to preserve existing rows.

Requires: cqlsh (from the cassandra client tools).
Reads:    PROOFLOAD_CASSANDRA_HOSTS      — comma-separated contact points
          PROOFLOAD_CASSANDRA_USER       — optional cqlsh username
          PROOFLOAD_CASSANDRA_PASSWORD   — optional cqlsh password
EOF
}

RESET=0
for arg in "$@"; do
    case "$arg" in
        --reset) RESET=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "provision.sh: unknown argument: $arg" >&2; usage; exit 2 ;;
    esac
done

if [[ -z "${PROOFLOAD_CASSANDRA_HOSTS:-}" ]]; then
    echo "provision.sh: PROOFLOAD_CASSANDRA_HOSTS is not set" >&2
    usage
    exit 2
fi

if ! command -v cqlsh >/dev/null 2>&1; then
    echo "provision.sh: required command not found: cqlsh" >&2
    exit 3
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/schema/schema.cql"
if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "provision.sh: schema file not found: $SCHEMA_FILE" >&2
    exit 3
fi

# Use the first contact point; split host:port into cqlsh's positional args.
FIRST="${PROOFLOAD_CASSANDRA_HOSTS%%,*}"
HOST="${FIRST%%:*}"
PORT="${FIRST##*:}"
if [[ "$PORT" == "$HOST" ]]; then
    PORT=9042
fi

CQLSH_ARGS=("$HOST" "$PORT")
if [[ -n "${PROOFLOAD_CASSANDRA_USER:-}" ]]; then
    CQLSH_ARGS+=(-u "$PROOFLOAD_CASSANDRA_USER" -p "${PROOFLOAD_CASSANDRA_PASSWORD:-}")
fi

echo "provision.sh: checking connectivity to $HOST:$PORT..."
if ! cqlsh "${CQLSH_ARGS[@]}" -e 'SELECT now() FROM system.local' >/dev/null 2>&1; then
    echo "provision.sh: cqlsh could not reach the cluster at $HOST:$PORT" >&2
    exit 4
fi

echo "provision.sh: applying schema..."
cqlsh "${CQLSH_ARGS[@]}" -f "$SCHEMA_FILE"

if [[ "$RESET" -eq 1 ]]; then
    echo "provision.sh: --reset given; truncating proofload.kv..."
    cqlsh "${CQLSH_ARGS[@]}" -e 'TRUNCATE proofload.kv'
else
    echo "provision.sh: preserving existing data (pass --reset to truncate)."
fi

echo "provision.sh: done."
