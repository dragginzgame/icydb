#!/usr/bin/env bash
set -euo pipefail

# Shared Tooling owns target execution. IcyDB retains a complete batch failure
# log in addition to the shared runner's per-target and highlighted evidence.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FAILURE_ROOT="${ICYDB_VALIDATION_FAILURE_LOG_DIR:-$ROOT/target/validation-failures}"

if [[ $# -eq 0 ]]; then
  echo "usage: run-icydb-validation-targets.sh [--fail-fast] <make-target>..." >&2
  exit 2
fi

mkdir -p "$FAILURE_ROOT"
RUN_LOG_DIR="$(mktemp -d "$FAILURE_ROOT/run.XXXXXX")"
status=0
VALIDATION_REPOSITORY_ROOT="$ROOT" VALIDATION_FAILURE_LOG_DIR="$RUN_LOG_DIR" \
  bash "$ROOT/scripts/ci/run-validation-targets.sh" "$@" || status=$?

if [[ "$status" -eq 0 ]]; then
  rmdir "$RUN_LOG_DIR"
  exit 0
fi

# The reviewed runner names retained raw logs with the zero-based target index.
# Keep those files and their printed paths intact; never aggregate its decorated
# summary or the last-target latest.log as if either were complete batch evidence.
if [[ "${1:-}" == "--fail-fast" ]]; then
  shift
fi
combined="$RUN_LOG_DIR/combined.log"
index=0
retained=0
for target in "$@"; do
  for log in "$RUN_LOG_DIR"/*-"$index"-*.log; do
    [[ -f "$log" ]] || continue
    printf '\n===== Target: %s =====\n\n' "$target" >> "$combined"
    cat "$log" >> "$combined"
    retained=$((retained + 1))
  done
  index=$((index + 1))
done

if [[ "$retained" -gt 0 ]]; then
  cp "$combined" "$FAILURE_ROOT/latest.log"
  cp "$RUN_LOG_DIR/latest-errors.log" "$FAILURE_ROOT/latest-errors.log"
  printf '\nCombined failure log retained at: %s\n' "$combined"
  printf 'Latest combined failure log: %s\n' "$FAILURE_ROOT/latest.log"
else
  rmdir "$RUN_LOG_DIR"
fi
exit "$status"
