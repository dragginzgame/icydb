#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"
require_rg "generated endpoint invariant checks"

status=0

# Capture discovery before iteration: process substitution would hide a failed
# traversal and could certify an empty/partial source set.
build_scripts="$(rg --files --hidden --no-ignore canisters --glob build.rs --glob '!**/target/**')"
while IFS= read -r build_script; do
  if [[ "$(rg_checked -c 'icydb::build::build_canister!\(' "$build_script")" != "1" ]]; then
    echo "[ERROR] $build_script must contain exactly one typed build_canister! invocation." >&2
    status=1
  fi
done <<< "$build_scripts"

for source in \
  canisters/audit/default_empty_metrics/src/lib.rs \
  canisters/audit/sql_perf/src/lib.rs \
  canisters/tests/sql/src/lib.rs \
  canisters/demo/rpg/src/lib.rs
do
  if ! rg -q 'canic_icydb::mount!\s*\{' "$source"; then
    echo "[ERROR] $source must own one explicit mount! block." >&2
    status=1
  fi
done

forbidden_exports="$(run_rg '#\[.*ic_cdk::(query|update|init|post_upgrade)|#\[.*export_name|#\[.*no_mangle' \
  crates/icydb-model/src/build/actor/db \
  crates/icydb-model/src/build/actor/mod.rs)"
if [[ -n "$forbidden_exports" ]]; then
  echo "[ERROR] generated private actor capabilities must not contain IC export attributes; start! owns composed lifecycle exports." >&2
  status=1
fi

# Host wrappers and their request scopes belong to canic-icydb. Installed
# endpoint tests qualify the selected adapter against these native capabilities.

if [[ $status -ne 0 ]]; then
  exit 1
fi

echo "[OK] Generated endpoint invariants verified."
