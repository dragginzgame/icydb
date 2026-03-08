#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

DB_ROOT="crates/icydb-core/src/db"
COMMON_GLOBS=(
  --glob '!**/tests/**'
  --glob '!**/tests.rs'
  --glob '!**/*_tests.rs'
  --glob '!**/test_*.rs'
)
ORDERING_AUDIT_DIRS=(
  "crates/icydb-core/src/db/executor"
  "crates/icydb-core/src/db/query"
  "crates/icydb-core/src/db/access"
  "crates/icydb-core/src/db/cursor"
)

# -----------------------------------------------------------------------------
# Complexity accretion baselines (update intentionally when ownership moves).
# -----------------------------------------------------------------------------

ACCESS_PATH_DECISION_OWNERS_BASELINE=5
ROUTE_SHAPE_DECISION_OWNERS_BASELINE=5
PREDICATE_COERCION_OWNERS_BASELINE=4

run_rg() {
  local pattern=$1
  shift
  rg -n --no-heading --color=never "$pattern" "$@" "${COMMON_GLOBS[@]}" || true
}

strip_comment_only() {
  awk -F: '{
    code=$0
    sub(/^[^:]+:[0-9]+:/, "", code)
    if (code ~ /^[[:space:]]*\/\//) {
      next
    }
    print $0
  }'
}

count_lines() {
  awk 'NF { count += 1 } END { print count + 0 }'
}

to_layers() {
  awk -F: '
    function layer_for(path) {
      if (path ~ /\/db\/query\//) return "query"
      if (path ~ /\/db\/access\//) return "access"
      if (path ~ /\/db\/executor\/route\//) return "route"
      if (path ~ /\/db\/executor\//) return "executor"
      if (path ~ /\/db\/index\//) return "index"
      if (path ~ /\/db\/commit\//) return "storage_commit"
      if (path ~ /\/db\/data\//) return "storage_data"
      if (path ~ /\/db\/codec\//) return "codec"
      if (path ~ /\/db\/cursor\//) return "cursor"
      return "other"
    }
    {
      code=$0
      sub(/^[^:]+:[0-9]+:/, "", code)
      if (code ~ /^[[:space:]]*\/\//) {
        next
      }
      print layer_for($1)
    }
  '
}

unique_layer_count() {
  to_layers | sort -u | count_lines
}

count_unique_owner_sites() {
  local scope=$1
  shift
  local matches=""
  local pattern
  for pattern in "$@"; do
    matches+=$(run_rg "$pattern" "$scope")
    matches+=$'\n'
  done
  printf '%s\n' "$matches" \
    | strip_comment_only \
    | awk -F: 'NF >= 2 { print $1 ":" $2 }' \
    | sort -u \
    | count_lines
}

status=0

# -----------------------------------------------------------------------------
# Strict semantic authority checks (fail on violation).
# -----------------------------------------------------------------------------

GROUPED_POLICY_PATTERN="GroupPlanError::OrderRequiresLimit|GroupPlanError::OrderPrefixNotAlignedWithGroupKeys|validate_group_cursor_constraints\\(|validate_grouped_distinct_policy\\(|validate_grouped_having_policy\\("
grouped_policy_leaks="$(
  run_rg "$GROUPED_POLICY_PATTERN" "$DB_ROOT" --glob '!crates/icydb-core/src/db/query/plan/**' \
    | strip_comment_only
)"
if [[ -n "$grouped_policy_leaks" ]]; then
  echo "[ERROR] Grouped policy legality must remain planner-owned (query/plan)." >&2
  echo "$grouped_policy_leaks" >&2
  status=1
fi

KEY_COMPARATOR_PATTERN="as_bytes\\(\\)\\.cmp\\(|\\b(RawIndexKey|IndexKey)\\b[^\\n]*\\.cmp\\("
key_comparator_leaks="$(
  run_rg "$KEY_COMPARATOR_PATTERN" "${ORDERING_AUDIT_DIRS[@]}" | strip_comment_only
)"
if [[ -n "$key_comparator_leaks" ]]; then
  echo "[ERROR] Index-key comparator logic must stay index-owned." >&2
  echo "$key_comparator_leaks" >&2
  status=1
fi

envelope_authority_leaks="$(
  run_rg "\\bfn\\s+(anchor_within_envelope|resume_bounds_from_refs|continuation_advanced)\\s*\\(" \
    "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/index/envelope.rs' \
    | strip_comment_only
)"
if [[ -n "$envelope_authority_leaks" ]]; then
  echo "[ERROR] Envelope/continuation authority must remain centralized in db/index/envelope.rs." >&2
  echo "$envelope_authority_leaks" >&2
  status=1
fi

commit_store_leaks="$(
  run_rg "with_commit_store\\(" "$DB_ROOT" --glob '!crates/icydb-core/src/db/commit/**' \
    | strip_comment_only
)"
if [[ -n "$commit_store_leaks" ]]; then
  echo "[ERROR] Commit marker low-level storage access must stay inside db/commit/*." >&2
  echo "$commit_store_leaks" >&2
  status=1
fi

continuation_rewrite_leaks="$(
  run_rg "\\bfn\\s+resume_bounds_from_refs\\s*\\(" "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/index/envelope.rs' \
    | strip_comment_only
)"
if [[ -n "$continuation_rewrite_leaks" ]]; then
  echo "[ERROR] Continuation bound rewrite logic must stay in db/index/envelope.rs." >&2
  echo "$continuation_rewrite_leaks" >&2
  status=1
fi

# -----------------------------------------------------------------------------
# Layer-health metrics (report for drift monitoring).
# -----------------------------------------------------------------------------

cross_layer_policy_rederivations="$(printf '%s\n' "$grouped_policy_leaks" | count_lines)"
comparator_outside_index="$(printf '%s\n' "$key_comparator_leaks" | count_lines)"
canonicalization_entrypoints="$(
  run_rg "\\bfn\\s+encode_canonical_index_component\\s*\\(" "$DB_ROOT" \
    | strip_comment_only \
    | count_lines
)"

upward_imports_tracked="$(
  run_rg "db::(query|executor|access)::" \
    "crates/icydb-core/src/db/index" \
    "crates/icydb-core/src/db/commit" \
    "crates/icydb-core/src/db/codec" \
    | strip_comment_only \
    | count_lines
)"

predicate_duplication_count=0
FEASIBILITY_PREDICATES=(
  "\\bfn\\s+allows_load_scan_budget_hint\\s*\\("
  "\\bfn\\s+allows_index_range_limit_pushdown\\s*\\("
  "\\bfn\\s+stream_order_contract_safe\\s*\\("
  "\\bfn\\s+derive_aggregate_execution_policy\\s*\\("
  "\\bfn\\s+is_index_range_limit_pushdown_shape_supported\\s*\\("
  "\\bfn\\s+index_range_limit_pushdown_shape_supported_for_order\\s*\\("
  "\\bfn\\s+direction_allows_physical_fetch_hint\\s*\\("
)
# These patterns intentionally track decision-owner function declarations.
# Field names and pass-through accessors are excluded to avoid false positives.
for predicate in "${FEASIBILITY_PREDICATES[@]}"; do
  layer_count="$(run_rg "$predicate" "$DB_ROOT" | unique_layer_count)"
  if [[ "$layer_count" -gt 1 ]]; then
    predicate_duplication_count=$((predicate_duplication_count + 1))
  fi
done

enum_fanout_gt2=0
enum_fanout_details=()
ENUM_TOKENS=(
  "AccessPath::"
  "AggregateKind::"
  "ContinuationMode::"
)
for token in "${ENUM_TOKENS[@]}"; do
  layer_count="$(run_rg "$token" "$DB_ROOT" | unique_layer_count)"
  if [[ "$layer_count" -gt 2 ]]; then
    enum_fanout_gt2=$((enum_fanout_gt2 + 1))
  fi
  enum_fanout_details+=("$token=$layer_count")
done

access_path_decision_owners="$(
  count_unique_owner_sites "$DB_ROOT" \
    "\\bfn\\s+derive_access_capabilities\\b" \
    "\\bfn\\s+access_plan_first_index_range_details_internal\\b" \
    "\\bfn\\s+access_plan_supports_reverse_traversal_internal\\b" \
    "\\bfn\\s+match_secondary_order_pushdown_core\\b" \
    "\\bfn\\s+index_range_limit_pushdown_shape_supported_for_order\\b"
)"

route_shape_decision_owners="$(
  count_unique_owner_sites "$DB_ROOT" \
    "\\bfn\\s+derive_route_intent_stage\\b" \
    "\\bfn\\s+derive_route_feasibility_stage\\b" \
    "\\bfn\\s+derive_route_execution_stage\\b" \
    "\\bfn\\s+derive_route_capabilities\\b" \
    "\\bfn\\s+derive_route_shape_kind\\b" \
    "\\bfn\\s+build_execution_route_plan\\b"
)"

predicate_coercion_decision_owners="$(
  count_unique_owner_sites "$DB_ROOT" \
    "\\bfn\\s+supports_coercion\\b" \
    "\\bfn\\s+canonicalize_compare_literal_for_coercion\\b" \
    "\\bfn\\s+normalize_compare_with_schema\\b" \
    "\\bfn\\s+eval_compare_values\\b"
)"

if [[ "$access_path_decision_owners" -gt "$ACCESS_PATH_DECISION_OWNERS_BASELINE" ]]; then
  echo "[ERROR] AccessPath decision-owner count drifted above baseline." >&2
  echo "        baseline=$ACCESS_PATH_DECISION_OWNERS_BASELINE current=$access_path_decision_owners" >&2
  status=1
fi

if [[ "$route_shape_decision_owners" -gt "$ROUTE_SHAPE_DECISION_OWNERS_BASELINE" ]]; then
  echo "[ERROR] RouteShape decision-owner count drifted above baseline." >&2
  echo "        baseline=$ROUTE_SHAPE_DECISION_OWNERS_BASELINE current=$route_shape_decision_owners" >&2
  status=1
fi

if [[ "$predicate_coercion_decision_owners" -gt "$PREDICATE_COERCION_OWNERS_BASELINE" ]]; then
  echo "[ERROR] Predicate-coercion decision-owner count drifted above baseline." >&2
  echo "        baseline=$PREDICATE_COERCION_OWNERS_BASELINE current=$predicate_coercion_decision_owners" >&2
  status=1
fi

echo "Layer Health Snapshot"
echo "  Upward imports (tracked edges): $upward_imports_tracked"
echo "  Cross-layer policy re-derivations: $cross_layer_policy_rederivations"
echo "  Cross-layer predicate duplication count: $predicate_duplication_count"
echo "  AccessPath decision owners: $access_path_decision_owners"
echo "  RouteShape decision owners: $route_shape_decision_owners"
echo "  Predicate coercion owners: $predicate_coercion_decision_owners"
echo "  Enum fan-out > 2 layers: $enum_fanout_gt2"
echo "  Enum layer counts: ${enum_fanout_details[*]}"
echo "  Comparator definitions outside index: $comparator_outside_index"
echo "  Canonicalization entrypoints: $canonicalization_entrypoints"

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Layer authority invariant checks failed." >&2
  exit 1
fi

echo "[OK] Layer authority invariants verified."
