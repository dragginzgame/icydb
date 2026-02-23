#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# Runtime entity-read modules that must stay slot/index based.
# Keep this list intentionally narrow so planner/setup code can evolve
# without tripping execution-path projection guards.
RUNTIME_FILES=(
  "crates/icydb-core/src/db/executor/load/aggregate_field.rs"
  "crates/icydb-core/src/db/executor/mutation/save/validation/invariants.rs"
  "crates/icydb-core/src/db/executor/mutation/save/validation/relations.rs"
  "crates/icydb-core/src/db/index/key/build.rs"
  "crates/icydb-core/src/db/index/plan/unique.rs"
  "crates/icydb-core/src/db/query/plan/logical/order_cursor.rs"
  "crates/icydb-core/src/db/query/predicate/eval.rs"
  "crates/icydb-core/src/db/relation/reverse_index.rs"
  "crates/icydb-core/src/model/entity.rs"
  "crates/icydb-core/src/traits/mod.rs"
)

status=0

method_call_matches="$({
  rg -n --no-heading --color=never "\\.get_value\\(" "${RUNTIME_FILES[@]}" || true
})"
if [[ -n "$method_call_matches" ]]; then
  echo "[ERROR] Runtime field projection must not call '.get_value('." >&2
  echo "$method_call_matches" >&2
  status=1
fi

method_decl_matches="$({
  rg -n --no-heading --color=never "\\bfn\\s+get_value\\s*\\(" "${RUNTIME_FILES[@]}" || true
})"
if [[ -n "$method_decl_matches" ]]; then
  echo "[ERROR] Runtime field projection must not define 'fn get_value(...)'." >&2
  echo "$method_decl_matches" >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Field projection invariants failed." >&2
  exit 1
fi

echo "[OK] Field projection invariants verified."
