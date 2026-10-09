#!/usr/bin/env bash
set -euo pipefail

# Testkit owns startup, readiness, process groups and reaping. IcyDB selects
# the deadline and complete output paths, relays signals and retains failures.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[[ "$#" -gt 0 ]] || { echo 'usage: run-with-pocketic-server.sh <command> [args...]' >&2; exit 2; }
runner="$(bash "$ROOT/scripts/ci/testkit-runner.sh" --check)"
selection=()
if [[ -z "${POCKET_IC_BIN:-}" ]]; then
  # Testkit distinguishes an absent override from a present empty variable.
  unset POCKET_IC_BIN
  selection=(--directory "$ROOT/.tools/ic-testkit-server")
fi
# This lane deliberately owns a fresh server, even if the invoking shell has a
# borrowed URL. Testkit rejects caller-owned output files with a borrowed URL.
unset IC_TESTKIT_POCKET_IC_URL
scratch_root="${TMPDIR:-$ROOT/.cache}"
mkdir -p "$scratch_root"
scratch="$(mktemp -d "$scratch_root/icydb-pocketic-server.XXXXXX")"
signal_status=0
runner_pid=""
trap 'signal_status=130; kill -INT "$runner_pid" 2>/dev/null || true' INT
trap 'signal_status=143; kill -TERM "$runner_pid" 2>/dev/null || true' TERM
"$runner" run ${selection[@]+"${selection[@]}"} --ttl 900 --startup-timeout 30 \
  --server-stdout "$scratch/stdout" --server-stderr "$scratch/stderr" -- "$@" &
runner_pid="$!"
# A signal may arrive between installing the traps and recording the child PID.
if [[ "$signal_status" != 0 ]]; then
  kill -"$((signal_status - 128))" "$runner_pid" 2>/dev/null || true
fi
status=0
wait "$runner_pid" || status=$?
if [[ "$signal_status" != 0 ]]; then
  # An interrupted wait returns before the runner finishes owned-group cleanup.
  trap '' INT TERM
  wait "$runner_pid" || true
  status="$signal_status"
fi
trap - INT TERM
if [[ "$status" != 0 ]]; then
  for stream in stderr stdout; do
    echo "==> shared PocketIC $stream (last 40 lines)" >&2
    tail -40 "$scratch/$stream" >&2 || true
  done
  echo "==> full PocketIC server logs retained: $scratch" >&2
else
  rm -f "$scratch/stdout" "$scratch/stderr"
  rmdir "$scratch"
fi
exit "$status"
