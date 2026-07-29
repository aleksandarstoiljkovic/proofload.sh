#!/usr/bin/env bash
#
# provision.sh — prepare an EXTERNAL Kafka cluster for a proofload run.
#
# Steps:
#   1. verify connectivity to $PROOFLOAD_KAFKA_BROKERS (list topics via the
#      Kafka admin CLI).
#   2. ensure the topic exists with the configured partitions/replication.
#   3. optionally DELETE and recreate the topic, but ONLY when --reset is
#      passed, so an existing log is never silently discarded.
#
# Usage:
#   PROOFLOAD_KAFKA_BROKERS="b1:9092,b2:9092" ./provision.sh [--reset]
#
# Optional environment (with defaults):
#   PROOFLOAD_KAFKA_TOPIC        (proofload)
#   PROOFLOAD_KAFKA_PARTITIONS   (12)
#   PROOFLOAD_KAFKA_REPLICATION  (3)
#
# Requires the Kafka topic admin CLI on PATH: kafka-topics.sh or kafka-topics.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: PROOFLOAD_KAFKA_BROKERS=<b1:9092,...> ./provision.sh [--reset]

  --reset   DELETE the topic (if present) and recreate it (destructive).
            Omit to preserve the existing topic and its data.

Requires: kafka-topics.sh or kafka-topics (from a Kafka distribution).
Reads:    PROOFLOAD_KAFKA_BROKERS       — comma-separated bootstrap brokers.
          PROOFLOAD_KAFKA_TOPIC         — topic name (default: proofload).
          PROOFLOAD_KAFKA_PARTITIONS    — partition count (default: 12).
          PROOFLOAD_KAFKA_REPLICATION   — replication factor (default: 3).
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

if [[ -z "${PROOFLOAD_KAFKA_BROKERS:-}" ]]; then
    echo "provision.sh: PROOFLOAD_KAFKA_BROKERS is not set" >&2
    usage
    exit 2
fi

TOPIC="${PROOFLOAD_KAFKA_TOPIC:-proofload}"
PARTITIONS="${PROOFLOAD_KAFKA_PARTITIONS:-12}"
REPLICATION="${PROOFLOAD_KAFKA_REPLICATION:-3}"

# Locate the topic admin CLI (script form in most distros, bare binary in some).
KAFKA_TOPICS=""
for bin in kafka-topics.sh kafka-topics; do
    if command -v "$bin" >/dev/null 2>&1; then
        KAFKA_TOPICS="$bin"
        break
    fi
done
if [[ -z "$KAFKA_TOPICS" ]]; then
    echo "provision.sh: required command not found: kafka-topics.sh (or kafka-topics)" >&2
    exit 3
fi

echo "provision.sh: checking connectivity to $PROOFLOAD_KAFKA_BROKERS..."
if ! "$KAFKA_TOPICS" --bootstrap-server "$PROOFLOAD_KAFKA_BROKERS" --list >/dev/null 2>&1; then
    echo "provision.sh: cannot reach brokers or list topics" >&2
    exit 4
fi

topic_exists() {
    "$KAFKA_TOPICS" --bootstrap-server "$PROOFLOAD_KAFKA_BROKERS" --list 2>/dev/null \
        | grep -Fxq "$TOPIC"
}

if [[ "$RESET" -eq 1 ]] && topic_exists; then
    echo "provision.sh: --reset given; deleting topic $TOPIC..."
    "$KAFKA_TOPICS" --bootstrap-server "$PROOFLOAD_KAFKA_BROKERS" --delete --topic "$TOPIC"
    # Deletion is asynchronous; wait for it to disappear before recreating.
    for _ in $(seq 1 30); do
        topic_exists || break
        sleep 1
    done
fi

if topic_exists; then
    echo "provision.sh: topic $TOPIC already exists; preserving it (pass --reset to recreate)."
else
    echo "provision.sh: creating topic $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION)..."
    "$KAFKA_TOPICS" --bootstrap-server "$PROOFLOAD_KAFKA_BROKERS" \
        --create --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION"
fi

echo "provision.sh: done."
