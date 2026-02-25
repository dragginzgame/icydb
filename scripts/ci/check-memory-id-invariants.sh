#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

status=0

REQUIRED_MATCHES=(
  $'crates/icydb-core/src/traits/mod.rs\tconst COMMIT_MEMORY_ID: u8;\tCanisterKind must define COMMIT_MEMORY_ID'
  $'crates/icydb-core/src/db/commit/recovery.rs\tconfigure_commit_memory_id\\(C::COMMIT_MEMORY_ID\\)\trecovery must configure explicit commit memory id from canister contract'
  $'crates/icydb-schema/src/node/canister.rs\tpub commit_memory_id: u8,\tschema canister node must carry commit_memory_id'
  $'crates/icydb-schema-derive/src/node/canister.rs\tpub commit_memory_id: u8,\tcanister derive node must require commit_memory_id'
)

FORBIDDEN_MATCHES=(
  $'crates/icydb-core/src/db/commit/memory.rs\tREGISTRY_DATA_STORE_LABEL\tcommit allocator must not depend on data store anchor labels'
  $'crates/icydb-core/src/db/commit/memory.rs\tREGISTRY_INDEX_STORE_LABEL\tcommit allocator must not depend on index store anchor labels'
  $'crates/icydb-core/src/db/commit/memory.rs\tsnapshot_ids_by_range\\(\\)\tcommit allocator must not scan range snapshots for anchor discovery'
  $'crates/icydb-core/src/db/commit/memory.rs\tunable to locate reserved memory range for commit markers\tlegacy anchor-discovery error must not remain'
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
