#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

GUARDED_ROOTS=(
  "crates/icydb-core/src/db/commit"
  "crates/icydb-core/src/db/executor/mutation"
  "crates/icydb-core/src/db/schema/mutation"
  "crates/icydb-core/src/db/schema/store"
  "crates/icydb-core/src/db/session/sql"
  "crates/icydb/src/db"
  "crates/icydb-build/src"
)

INTERLEAVING_PATTERN="\\basync\\s+fn\\b|\\basync\\s+move\\b|\\.await\\b|\\bic_cdk::(call|spawn)\\b|\\bcall_raw\\b|\\bnotify_raw\\b|\\bic_cdk_timers\\b|\\bset_timer(_interval)?\\b"

interleaving_points="$(
  rg -n --no-heading --color=never "$INTERLEAVING_PATTERN" \
    "${GUARDED_ROOTS[@]}" \
    "${COMMON_GLOBS[@]}" \
    | strip_comment_only \
    || true
)"

if [[ -n "$interleaving_points" ]]; then
  echo "[ERROR] Mutation/publication paths must remain synchronous and non-reentrant." >&2
  echo "[ERROR] Do not introduce async/await, spawned work, timers, or canister calls inside guarded mutation paths." >&2
  echo "$interleaving_points" >&2
  exit 1
fi

echo "[OK] Mutation atomicity invariants verified."
