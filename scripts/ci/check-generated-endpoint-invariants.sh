#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "[ERROR] ripgrep (rg) is required for generated endpoint invariant checks." >&2
  exit 1
fi

status=0

while IFS= read -r -d '' build_script; do
  if [[ "$(rg -c 'icydb::build::build_canister!\(' "$build_script")" != "1" ]]; then
    echo "[ERROR] $build_script must contain exactly one typed build_canister! invocation." >&2
    status=1
  fi
done < <(find canisters -name build.rs -print0)

for source in \
  canisters/audit/default_empty_metrics/src/lib.rs \
  canisters/audit/sql_perf/src/lib.rs \
  canisters/test/sql/src/lib.rs \
  canisters/demo/rpg/src/lib.rs
do
  if ! rg -q 'icydb::endpoints!\s*\{' "$source"; then
    echo "[ERROR] $source must own one explicit endpoints! block." >&2
    status=1
  fi
done

if rg -q '#\[.*ic_cdk::(query|update)|#\[.*export_name|#\[.*no_mangle' \
  crates/icydb-model/src/build/actor/db \
  crates/icydb-model/src/build/actor/mod.rs
then
  echo "[ERROR] generated private actor capabilities must not contain IC export attributes." >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  exit 1
fi

echo "[OK] Generated endpoint invariants verified."
