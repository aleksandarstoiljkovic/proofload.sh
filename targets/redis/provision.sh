#!/usr/bin/env bash
#
# provision.sh — prepare an EXTERNAL Redis for a proofload run.
#
# Redis is schemaless, so there is no DDL to apply. Provisioning therefore only:
#   1. verifies connectivity to $PROOFLOAD_REDIS_ADDR via redis-cli PING
#   2. optionally FLUSHDB, but ONLY when --reset is passed, so existing data is
#      never silently destroyed.
#
# Usage:
#   PROOFLOAD_REDIS_ADDR="host:port" ./provision.sh [--reset]
#
# Auth: if the server requires a password, export PROOFLOAD_REDIS_PASSWORD (or
# REDIS_PASSWORD); it is passed to redis-cli via REDISCLI_AUTH so it never
# appears in the process argument list.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: PROOFLOAD_REDIS_ADDR=<host:port> ./provision.sh [--reset]

  --reset   FLUSHDB after the connectivity check (destructive: wipes DB 0).
            Omit to preserve existing keys.

Requires: redis-cli (from the redis client package).
Reads:    PROOFLOAD_REDIS_ADDR        — target host:port
          PROOFLOAD_REDIS_PASSWORD /
          REDIS_PASSWORD              — optional auth password
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

if [[ -z "${PROOFLOAD_REDIS_ADDR:-}" ]]; then
    echo "provision.sh: PROOFLOAD_REDIS_ADDR is not set" >&2
    usage
    exit 2
fi

if ! command -v redis-cli >/dev/null 2>&1; then
    echo "provision.sh: required command not found: redis-cli" >&2
    exit 3
fi

# Split host:port on the last colon so bracketed IPv6 literals survive.
HOST="${PROOFLOAD_REDIS_ADDR%:*}"
PORT="${PROOFLOAD_REDIS_ADDR##*:}"
if [[ "$HOST" == "$PROOFLOAD_REDIS_ADDR" ]]; then
    PORT="6379"  # no colon present; use default port
fi

# redis-cli reads REDISCLI_AUTH so the password stays off the command line.
if [[ -n "${PROOFLOAD_REDIS_PASSWORD:-}" ]]; then
    export REDISCLI_AUTH="$PROOFLOAD_REDIS_PASSWORD"
elif [[ -n "${REDIS_PASSWORD:-}" ]]; then
    export REDISCLI_AUTH="$REDIS_PASSWORD"
fi

echo "provision.sh: checking connectivity to ${HOST}:${PORT}..."
PONG="$(redis-cli -h "$HOST" -p "$PORT" PING 2>/dev/null || true)"
if [[ "$PONG" != "PONG" ]]; then
    echo "provision.sh: server did not answer PING (got: '${PONG}')" >&2
    exit 4
fi

if [[ "$RESET" -eq 1 ]]; then
    echo "provision.sh: --reset given; flushing DB 0..."
    redis-cli -h "$HOST" -p "$PORT" FLUSHDB >/dev/null
else
    echo "provision.sh: preserving existing data (pass --reset to flush)."
fi

echo "provision.sh: done."
