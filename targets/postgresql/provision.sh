#!/usr/bin/env bash
#
# provision.sh — prepare an EXTERNAL PostgreSQL for a proofload run.
#
# Steps:
#   1. verify connectivity to $PROOFLOAD_PG_DSN (pg_isready, then a psql probe)
#   2. apply schema/schema.sql (idempotent CREATE TABLE IF NOT EXISTS)
#   3. optionally TRUNCATE proofload_kv, but ONLY when --reset is passed, so a
#      dirty table is never silently clobbered.
#
# Usage:
#   PROOFLOAD_PG_DSN="postgres://user:pass@host:5432/proofload" ./provision.sh [--reset]
#
# The DSN may be a URL or a libpq keyword string; psql accepts both.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: PROOFLOAD_PG_DSN=<dsn> ./provision.sh [--reset]

  --reset   TRUNCATE proofload_kv after applying the schema (destructive).
            Omit to preserve existing rows.

Requires: psql, pg_isready (from the postgresql client package).
Reads:    PROOFLOAD_PG_DSN — libpq connection string (URL or keyword form).
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

if [[ -z "${PROOFLOAD_PG_DSN:-}" ]]; then
    echo "provision.sh: PROOFLOAD_PG_DSN is not set" >&2
    usage
    exit 2
fi

for bin in psql pg_isready; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "provision.sh: required command not found: $bin" >&2
        exit 3
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/schema/schema.sql"
if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "provision.sh: schema file not found: $SCHEMA_FILE" >&2
    exit 3
fi

echo "provision.sh: checking connectivity..."
if ! pg_isready -d "$PROOFLOAD_PG_DSN" >/dev/null 2>&1; then
    echo "provision.sh: pg_isready reports the server is not accepting connections" >&2
    exit 4
fi
# Confirm we can actually authenticate and run a statement.
psql "$PROOFLOAD_PG_DSN" -v ON_ERROR_STOP=1 -tAc 'SELECT 1' >/dev/null

echo "provision.sh: applying schema..."
psql "$PROOFLOAD_PG_DSN" -v ON_ERROR_STOP=1 -f "$SCHEMA_FILE"

if [[ "$RESET" -eq 1 ]]; then
    echo "provision.sh: --reset given; truncating proofload_kv..."
    psql "$PROOFLOAD_PG_DSN" -v ON_ERROR_STOP=1 -c 'TRUNCATE TABLE proofload_kv'
else
    echo "provision.sh: preserving existing data (pass --reset to truncate)."
fi

echo "provision.sh: done."
