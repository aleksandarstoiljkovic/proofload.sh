#!/usr/bin/env bash
#
# provision.sh — external-backend setup/verification for a ClickHouse target that
# proofload only connects to (i.e. NOT provisioned by proofload). It checks
# connectivity, applies the schema, and — only with an explicit --reset — clears
# the table. proofload never resets data silently.
#
# Env:
#   PROOFLOAD_CLICKHOUSE_ADDR   host:port (native, default 127.0.0.1:9000)
#   PROOFLOAD_CLICKHOUSE_PASSWORD / CLICKHOUSE_PASSWORD   optional password
#   CLICKHOUSE_USER             username (default "default")
#
# Usage: provision.sh [--reset]
set -euo pipefail

ADDR="${PROOFLOAD_CLICKHOUSE_ADDR:-127.0.0.1:9000}"
HOST="${ADDR%:*}"; PORT="${ADDR##*:}"
USER="${CLICKHOUSE_USER:-default}"
PASS="${PROOFLOAD_CLICKHOUSE_PASSWORD:-${CLICKHOUSE_PASSWORD:-}}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESET=0
[[ "${1:-}" == "--reset" ]] && RESET=1

command -v clickhouse-client >/dev/null 2>&1 || {
    echo "provision.sh: clickhouse-client not found on PATH" >&2; exit 1; }

ch() { clickhouse-client --host "$HOST" --port "$PORT" --user "$USER" \
    ${PASS:+--password "$PASS"} "$@"; }

echo "provision.sh: checking connectivity to $ADDR ..."
ch --query "SELECT 1" >/dev/null

echo "provision.sh: applying schema ..."
ch --multiquery < "$DIR/schema/schema.sql"

if [[ "$RESET" -eq 1 ]]; then
    echo "provision.sh: --reset -> TRUNCATE proofload_kv"
    ch --query "TRUNCATE TABLE IF EXISTS proofload_kv" || true
fi
echo "provision.sh: ok"
