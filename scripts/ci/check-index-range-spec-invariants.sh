#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# Index-path executor files must stay mechanical (raw key bytes only).
INDEX_EXECUTOR_FILES=(
  "crates/icydb-core/src/db/executor/stream/access/mod.rs"
  "crates/icydb-core/src/db/executor/stream/access/bindings.rs"
  "crates/icydb-core/src/db/executor/stream/access/traversal.rs"
  "crates/icydb-core/src/db/executor/stream/access/physical.rs"
  "crates/icydb-core/src/db/executor/stream/access/scan.rs"
  "crates/icydb-core/src/db/executor/scan/index_range_limit.rs"
  "crates/icydb-core/src/db/executor/scan/secondary_index.rs"
  "crates/icydb-core/src/db/executor/scan/fast_stream_route/handlers.rs"
  "crates/icydb-core/src/db/executor/pipeline/runtime/mod.rs"
  "crates/icydb-core/src/db/executor/pipeline/runtime/fast_path/mod.rs"
  "crates/icydb-core/src/db/executor/pipeline/runtime/fast_path/strategy.rs"
  "crates/icydb-core/src/db/executor/delete/mod.rs"
)

# These patterns indicate semantic fallback logic creeping back into execution.
FORBIDDEN_PATTERNS=(
  "\\bValue\\b"
  "encode_canonical_index_component"
  "raw_bounds_for_encoded_index_component_range"
  "resolve_data_values_in_range"
  "index_range_bounds_from_values"
)

# Non-test/non-mutation executor runtime must remain free of semantic
# index-order encoding helpers.
EXECUTOR_RUNTIME_FORBIDDEN_PATTERNS=(
  "\\bEncodedValue\\b"
  "try_from_ref"
)

# Required guardrails that enforce the planner->executor lowering contract.
REQUIRED_MATCHES=(
  "crates/icydb-core/src/db/executor/stream/access/bindings.rs:::unused index-prefix executable specs after access-plan traversal:::missing invariant check for unused IndexPrefixSpec entries"
  "crates/icydb-core/src/db/executor/stream/access/physical.rs:::index-prefix execution requires pre-lowered index-prefix spec:::missing invariant error for unresolved index-prefix specs in physical path resolution"
  "crates/icydb-core/src/db/executor/stream/access/traversal.rs:::index-prefix spec does not match access path index:::missing invariant error for misaligned index-prefix specs in physical path resolution"
  "crates/icydb-core/src/db/executor/scan/fast_stream_route/handlers.rs:::index-prefix executable spec must be materialized for index-prefix plans:::missing invariant error for unresolved secondary index prefix specs"
  "crates/icydb-core/src/db/executor/stream/access/scan.rs:::resolve_data_values_in_raw_range_limited:::index-prefix physical execution must use raw-range traversal"
  "crates/icydb-core/src/db/executor/stream/access/traversal.rs:::index-prefix spec does not match access path index:::missing invariant error for misaligned IndexPrefixSpec consumption"
  "crates/icydb-core/src/db/executor/route/guard.rs:::secondary fast-path resolution expects at most one index-prefix spec:::missing invariant error for multi-spec secondary fast-path drift"
  "crates/icydb-core/src/db/executor/stream/access/bindings.rs:::validate_index_range_specs_consumed:::missing invariant check for unused IndexRangeSpec entries"
  "crates/icydb-core/src/db/executor/stream/access/physical.rs:::require_index_range_spec:::missing invariant check for unresolved index-range specs in physical path resolution"
  "crates/icydb-core/src/db/executor/traversal.rs:::index-range execution requires pre-lowered index-range spec:::missing invariant error for unresolved index-range specs at shared traversal boundary"
  "crates/icydb-core/src/db/executor/traversal.rs:::index-range spec does not match access path index:::missing invariant error for misaligned index-range specs at shared traversal boundary"
  "crates/icydb-core/src/db/executor/scan/fast_stream_route/handlers.rs:::index-range executable spec must be materialized for index-range plans:::missing invariant error for unresolved index-range pushdown specs"
  "crates/icydb-core/src/db/executor/stream/access/scan.rs:::resolve_data_values_in_raw_range_limited:::index-range physical execution must use raw-range traversal"
  "crates/icydb-core/src/db/executor/stream/access/traversal.rs:::validate_index_range_spec_alignment:::missing invariant check for misaligned IndexRangeSpec consumption"
  "crates/icydb-core/src/db/executor/route/guard.rs:::index-range fast-path resolution expects at most one index-range spec:::missing invariant error for multi-spec index-range fast-path drift"
)

status=0

if ! command -v rg >/dev/null 2>&1; then
  echo "[ERROR] ripgrep (rg) is required for index-range spec invariant checks." >&2
  echo "[ERROR] Install it via 'make install-dev' or scripts/env/install.sh." >&2
  exit 1
fi

for file in "${INDEX_EXECUTOR_FILES[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing expected executor file: $file" >&2
    status=1
  fi
done

for check in "${REQUIRED_MATCHES[@]}"; do
  file="${check%%:::*}"
  rest="${check#*:::}"
  pattern="${rest%%:::*}"
  message="${rest#*:::}"
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing required check file: $file" >&2
    status=1
    continue
  fi
  if ! rg -n --no-heading --color=never "$pattern" "$file" >/dev/null; then
    echo "[ERROR] $message ($file, pattern: $pattern)" >&2
    status=1
  fi
done

for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
  matches="$(rg -n --no-heading --color=never "$pattern" "${INDEX_EXECUTOR_FILES[@]}" || true)"
  if [[ -n "$matches" ]]; then
    echo "[ERROR] Executor index-path invariant violated: found forbidden pattern '$pattern'" >&2
    echo "$matches" >&2
    status=1
  fi
done

for pattern in "${EXECUTOR_RUNTIME_FORBIDDEN_PATTERNS[@]}"; do
  matches="$(
    rg -n --no-heading --color=never "$pattern" \
      crates/icydb-core/src/db/executor \
      --glob '!**/tests/**' \
      --glob '!**/mutation/**' \
      || true
  )"
  if [[ -n "$matches" ]]; then
    echo "[ERROR] Executor runtime must stay byte-only: found forbidden pattern '$pattern'" >&2
    echo "$matches" >&2
    status=1
  fi
done

lookup_value_matches="$(
  rg -n --no-heading --color=never "\\bValue\\b" \
    crates/icydb-core/src/db/index/store.rs \
    || true
)"
if [[ -n "$lookup_value_matches" ]]; then
  echo "[ERROR] Index traversal lookup must stay raw-key only (found Value usage)." >&2
  echo "$lookup_value_matches" >&2
  status=1
fi

prefix_builder_calls_outside_index="$(
  rg -n --no-heading --color=never \
    "IndexKey::bounds_for_prefix(_with_kind|_component_range(_with_kind)?)?\\(" \
    crates/icydb-core/src/db \
    --glob '!crates/icydb-core/src/db/index/**' \
    || true
)"
if [[ -n "$prefix_builder_calls_outside_index" ]]; then
  echo "[ERROR] Raw prefix/range key builder calls must stay inside db/index." >&2
  echo "$prefix_builder_calls_outside_index" >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Index-range spec invariant checks failed." >&2
  exit 1
fi

echo "[OK] Index-range spec invariants verified."
