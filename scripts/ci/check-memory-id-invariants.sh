#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

status=0

REQUIRED_MATCHES=(
  $'crates/icydb-core/src/traits/mod.rs\tconst COMMIT_MEMORY_ID: u8;\tCanisterKind must define COMMIT_MEMORY_ID'
  $'crates/icydb-core/src/traits/mod.rs\tconst COMMIT_STABLE_KEY: &\'static str;\tCanisterKind must define COMMIT_STABLE_KEY'
  $'crates/icydb-core/src/db/commit/recovery.rs\tconfigure_commit_memory_id\\(C::COMMIT_MEMORY_ID, C::COMMIT_STABLE_KEY\\)\trecovery must configure explicit commit memory id and stable key from canister contract'
  $'crates/icydb-model/src/node/canister.rs\tcommit_memory_id: u8,\tmodel canister node must carry commit_memory_id'
  $'crates/icydb-model/src/node/canister.rs\tmemory_namespace: &\'static str,\tmodel canister node must carry memory_namespace'
  $'crates/icydb-model/src/node/mod.rs\tpub const APP_MEMORY_ID_MIN: u8 = 100;\tmodel must retain the canonical application memory-id floor'
  $'crates/icydb-model/src/node/mod.rs\tpub const APP_MEMORY_ID_MAX: u8 = 254;\tmodel must retain the canonical application memory-id ceiling'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const APP_MEMORY_ID_MIN: u8 = 100;\truntime convergence must retain the generated application memory-id floor'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const APP_MEMORY_ID_MAX: u8 = 254;\truntime convergence must retain the generated application memory-id ceiling'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const CANISTER_CONTROL_ALLOCATION_COUNT: usize = 3;\tconvergence must reserve the three generated canister controls'
  $'crates/icydb-core/src/db/database_format/convergence.rs\tpub\(in crate::db\) const JOURNALED_STORE_ALLOCATION_WIDTH: usize = 4;\tconvergence must charge all four journaled-store allocations'
  $'crates/icydb-core/src/db/commit/store/control_slot.rs\tpub\(in crate::db\) const MAX_PERSISTED_STORE_ALLOCATIONS: usize = 16;\tcommit control must retain the hard-cut 16-store registry ceiling'
)

FORBIDDEN_MATCHES=(
  $'crates/icydb-core/src/db/commit/memory.rs\tREGISTRY_DATA_STORE_LABEL\tcommit allocator must not depend on data store anchor labels'
  $'crates/icydb-core/src/db/commit/memory.rs\tREGISTRY_INDEX_STORE_LABEL\tcommit allocator must not depend on index store anchor labels'
  $'crates/icydb-core/src/db/commit/memory.rs\tsnapshot_ids_by_range\\(\\)\tcommit allocator must not scan range snapshots for anchor discovery'
)

for check in "${REQUIRED_MATCHES[@]}"; do
  IFS=$'\t' read -r file pattern message <<<"$check"
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing required file: $file" >&2
    status=1
    continue
  fi

  if ! rg -n --no-heading --color=never "$pattern" "$file" >/dev/null; then
    echo "[ERROR] $message ($file, pattern: $pattern)" >&2
    status=1
  fi
done

for check in "${FORBIDDEN_MATCHES[@]}"; do
  IFS=$'\t' read -r file pattern message <<<"$check"
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing required file: $file" >&2
    status=1
    continue
  fi

  matches="$(rg -n --no-heading --color=never "$pattern" "$file" || true)"
  if [[ -n "$matches" ]]; then
    echo "[ERROR] $message ($file, pattern: $pattern)" >&2
    echo "$matches" >&2
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Memory-id invariant checks failed." >&2
  exit 1
fi

echo "[OK] Memory-id invariants verified."
