#!/usr/bin/env bash
# proofload — single entrypoint that dispatches to per-target Go engines.
#
# Each database/broker lives in targets/<name>/ and builds to a self-contained
# binary bin/proofload-<name>. This script resolves the target, builds its engine
# on demand, and hands the remaining arguments to it.
#
# Usage:
#   ./proofload.sh run   <target> <workload> [engine flags...]
#   ./proofload.sh schema <target> [engine flags...]
#   ./proofload.sh build [target]        # build one target, or all
#   ./proofload.sh list                  # list available targets
#   ./proofload.sh <target> <subcommand> [flags...]   # low-level passthrough
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
BIN="$ROOT/bin"

die() { echo "proofload: $*" >&2; exit 1; }

list_targets() {
  find targets -maxdepth 1 -mindepth 1 -type d -printf '%f\n' 2>/dev/null | sort
}

build_target() {
  local t="$1"
  [ -d "targets/$t" ] || die "unknown target '$t' (have: $(list_targets | paste -sd, -))"
  mkdir -p "$BIN"
  go build -o "$BIN/proofload-$t" "./targets/$t"
}

engine() {
  local t="$1"; shift
  build_target "$t"
  exec "$BIN/proofload-$t" "$@"
}

[ $# -ge 1 ] || die "usage: ./proofload.sh {run|schema|build|list|<target>} ..."
cmd="$1"; shift

case "$cmd" in
  list)  list_targets ;;
  build)
    if [ $# -ge 1 ]; then build_target "$1"; echo "built bin/proofload-$1";
    else for t in $(list_targets); do build_target "$t"; echo "built bin/proofload-$t"; done; fi ;;
  run)
    [ $# -ge 2 ] || die "usage: ./proofload.sh run <target> <workload> [flags...]"
    t="$1"; wl="$2"; shift 2
    engine "$t" run --workload "$wl" "$@" ;;
  schema)
    [ $# -ge 1 ] || die "usage: ./proofload.sh schema <target> [flags...]"
    t="$1"; shift
    engine "$t" schema "$@" ;;
  *)
    # low-level passthrough: proofload.sh <target> <subcommand> [flags...]
    engine "$cmd" "$@" ;;
esac
