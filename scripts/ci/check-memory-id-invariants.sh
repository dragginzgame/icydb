#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

status=0

REQUIRED_MATCHES=(
  $'crates/icydb-core/src/traits/mod.rs\tfn commit_memory_id\\(\\) -> Result<u64, ic_storage::RuntimeOpenError>\tCanisterKind must resolve the committed control allocation fallibly'
  $'crates/icydb-core/src/traits/mod.rs\tconst COMMIT_STABLE_KEY: &\'static str;\tCanisterKind must define COMMIT_STABLE_KEY'
  $'crates/icydb-core/src/db/commit/recovery.rs\tC::commit_memory_id\\(\\)\trecovery must resolve its committed control ID'
  $'crates/icydb-core/src/traits/mod.rs\tallocation_id\\(Self::COMMIT_STABLE_KEY\\)\tcanister controls resolve the bound named allocation'
  $'crates/icydb-core/src/db/registry/handle.rs\tcrate::db::memory_runtime::allocation_id\\(stable_key\\)\tstore identities resolve the bound named allocation'
  $'crates/icydb-model/src/node/store.rs\tkey: &\'static str,\tjournaled stores must carry a permanent logical key'
  $'crates/icydb-model/src/node/canister.rs\tmemory_namespace: &\'static str,\tmodel canister node must carry memory_namespace'
  $'crates/icydb/src/db/bootstrap.rs\tprepare_memory_bootstrap\\(self.0, admission\\)\tstandalone bootstrap must use shared logical admission'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const CANISTER_CONTROL_ALLOCATION_COUNT: usize = 3;\tconvergence must reserve the three generated canister controls'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const JOURNALED_STORE_ALLOCATION_WIDTH: usize = 4;\tconvergence must charge all four journaled-store allocations'
  $'crates/icydb-core/src/db/commit/store/control_slot.rs\tpub\(in crate::db\) const MAX_PERSISTED_STORE_ALLOCATIONS: usize = 16;\tcommit control must retain the hard-cut 16-store registry ceiling'
)

for check in "${REQUIRED_MATCHES[@]}"; do
  IFS=$'\t' read -r file pattern message <<<"$check"
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing required file: $file" >&2
    status=1
    continue
  fi

  if ! rg -n -U --no-heading --color=never "$pattern" "$file" >/dev/null; then
    echo "[ERROR] $message ($file, pattern: $pattern)" >&2
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Memory-id invariant checks failed." >&2
  exit 1
fi

echo "[OK] Memory-id invariants verified."
