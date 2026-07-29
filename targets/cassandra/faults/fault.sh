#!/usr/bin/env bash
#
# fault.sh — inject and heal faults on a Cassandra cluster node for the
# proofload nemesis. Driven by core/nemesis.ScriptController.
#
# Argv contract (built by ScriptController.buildArgs):
#   inject: fault.sh inject --method <m> <refflag> <ref> [--namespace <ns>] \
#           --type <faultType> [--param key=value ...]
#   heal:   fault.sh heal   --method <m> <refflag> <ref> [--namespace <ns>]
# where <refflag> is --pod (k8s), --container (docker) or --host (ssh).
#
# heal carries no --type (the FaultController contract supplies none), so it
# reverses every applied effect best-effort.
#
# Exit codes: 0 ok; 2 usage/unknown action; 3 unsupported type for the method.
set -euo pipefail

TARGET="cassandra"
PROG="$(basename "$0")"

usage() {
    cat >&2 <<EOF
usage: $PROG <inject|heal> --method <k8s|docker|ssh> <--pod|--container|--host> <ref>
             [--namespace <ns>] [--type <faultType>] [--param key=value ...]

faultTypes: kill-node pause-node network-partition clock-skew flush
extra: flush (nodetool flush; docker/k8s/ssh). isr-shrink is unsupported.

Requires the matching CLI for the method: kubectl / docker / ssh.
EOF
}

# --- parse -----------------------------------------------------------------
[[ $# -ge 1 ]] || { usage; exit 2; }
[[ "$1" == "-h" || "$1" == "--help" ]] && { usage; exit 0; }
ACTION="$1"; shift
METHOD="" REF="" NAMESPACE="" TYPE=""
declare -a PARAMS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --method) METHOD="$2"; shift 2 ;;
        --pod|--container|--host|--ref) REF="$2"; shift 2 ;;
        --namespace) NAMESPACE="$2"; shift 2 ;;
        --type) TYPE="$2"; shift 2 ;;
        --param) PARAMS+=("$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "$PROG: unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done
[[ -n "$METHOD" && -n "$REF" ]] || { echo "$PROG: --method and a node ref are required" >&2; usage; exit 2; }

# param KEY [DEFAULT] -> value of --param KEY=... or DEFAULT.
param() {
    local key="$1" def="${2:-}" kv
    for kv in ${PARAMS[@]+"${PARAMS[@]}"}; do
        [[ "$kv" == "$key="* ]] && { echo "${kv#*=}"; return 0; }
    done
    echo "$def"
}

# k/d/s wrap a command execution on the node for each control method.
kexec() { kubectl -n "${NAMESPACE:-default}" exec "$REF" -- "$@"; }
dexec() { docker exec "$REF" "$@"; }
sexec() { ssh "$REF" "$@"; }
NETPOL="proofload-partition-$REF"

# --- fault primitives ------------------------------------------------------
do_kill() {
    case "$METHOD" in
        docker) docker kill "$REF" ;;
        k8s)    kubectl -n "${NAMESPACE:-default}" delete pod "$REF" --grace-period=0 --force ;;
        ssh)    sexec "sudo pkill -9 -f '$(param proc cassandra)'" ;;
        *) return 3 ;;
    esac
}
do_pause() {
    case "$METHOD" in
        docker) docker pause "$REF" ;;
        k8s)    kexec sh -c 'kill -STOP -1' ;;
        ssh)    sexec "sudo pkill -STOP -f '$(param proc cassandra)'" ;;
        *) return 3 ;;
    esac
}
do_partition() {
    case "$METHOD" in
        docker) docker network disconnect "$(param network bridge)" "$REF" ;;
        k8s)    kubectl -n "${NAMESPACE:-default}" apply -f - <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: $NETPOL }
spec:
  podSelector: { matchLabels: { statefulset.kubernetes.io/pod-name: $REF } }
  policyTypes: [Ingress, Egress]
YAML
                ;;
        ssh)    sexec "sudo iptables -A INPUT -j DROP && sudo iptables -A OUTPUT -j DROP" ;;
        *) return 3 ;;
    esac
}
# clock-skew is best-effort: it needs a writable clock (privileged container /
# host) and a date(1) that accepts -s; where unavailable it fails non-zero.
do_clock_skew() {
    local secs; secs="$(param skew 300)"
    case "$METHOD" in
        docker) dexec sh -c "date -s \"@\$(( \$(date +%s) + $secs ))\"" ;;
        k8s)    kexec sh -c "date -s \"@\$(( \$(date +%s) + $secs ))\"" ;;
        ssh)    sexec "sudo date -s \"@\$(( \$(date +%s) + $secs ))\"" ;;
        *) return 3 ;;
    esac
}

# target_inject handles $TARGET-specific fault types; returns 3 for unsupported.
# flush forces a memtable flush (nodetool flush) so on-disk state is exercised;
# it self-heals (nothing to undo), so heal is a no-op for it.
target_inject() {
    case "$TYPE" in
        flush)
            case "$METHOD" in
                docker) dexec nodetool flush ;;
                k8s)    kexec nodetool flush ;;
                ssh)    sexec "nodetool flush" ;;
                *) return 3 ;;
            esac
            ;;
        *)
            echo "$PROG: unsupported fault type for $TARGET/$METHOD: $TYPE" >&2
            return 3
            ;;
    esac
}

# --- dispatch --------------------------------------------------------------
inject() {
    case "$TYPE" in
        kill-node)         do_kill ;;
        pause-node)        do_pause ;;
        network-partition) do_partition ;;
        clock-skew)        do_clock_skew ;;
        "") echo "$PROG: inject requires --type" >&2; usage; exit 2 ;;
        *)  target_inject ;;
    esac
}

# heal reverses every effect best-effort; unknown-but-not-applied undo steps are
# expected to fail and are ignored so the node is always returned to health.
heal() {
    case "$METHOD" in
        docker)
            docker unpause "$REF" 2>/dev/null || true
            docker start "$REF" 2>/dev/null || true
            docker network connect "$(param network bridge)" "$REF" 2>/dev/null || true
            ;;
        k8s)
            kexec sh -c 'kill -CONT -1' 2>/dev/null || true
            kubectl -n "${NAMESPACE:-default}" delete networkpolicy "$NETPOL" --ignore-not-found 2>/dev/null || true
            ;;
        ssh)
            sexec "sudo pkill -CONT -f '$(param proc cassandra)'" 2>/dev/null || true
            sexec "sudo iptables -D INPUT -j DROP; sudo iptables -D OUTPUT -j DROP" 2>/dev/null || true
            ;;
        *) echo "$PROG: unknown method: $METHOD" >&2; exit 2 ;;
    esac
}

case "$ACTION" in
    inject) inject ;;
    heal)   heal ;;
    *) echo "$PROG: unknown action: $ACTION" >&2; usage; exit 2 ;;
esac
