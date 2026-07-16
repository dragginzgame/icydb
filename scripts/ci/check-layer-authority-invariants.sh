#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

CORE_ROOT="crates/icydb-core/src"
DB_ROOT="crates/icydb-core/src/db"
ROUTE_PLANNER_ROOT="crates/icydb-core/src/db/executor/planning/route/planner/mod.rs"
ALLOWED_TOP_LEVEL_DB_FAMILIES=3
ORDERING_AUDIT_DIRS=(
  "crates/icydb-core/src/db/executor"
  "crates/icydb-core/src/db/query"
  "crates/icydb-core/src/db/access"
  "crates/icydb-core/src/db/cursor"
)
FIELD_PROJECTION_RUNTIME_FILES=(
  "crates/icydb-core/src/db/executor/aggregate/projection/mod.rs"
  "crates/icydb-core/src/db/executor/delete/post_access.rs"
  "crates/icydb-core/src/db/executor/mutation/save_validation.rs"
  "crates/icydb-core/src/db/executor/order.rs"
  "crates/icydb-core/src/db/executor/terminal/page/post_access.rs"
  "crates/icydb-core/src/db/index/key/build.rs"
  "crates/icydb-core/src/db/index/plan/unique.rs"
  "crates/icydb-core/src/db/predicate/runtime/mod.rs"
  "crates/icydb-core/src/db/relation/reverse_index.rs"
  "crates/icydb-core/src/model/entity.rs"
  "crates/icydb-core/src/traits/mod.rs"
)

status=0

# -----------------------------------------------------------------------------
# Strict semantic authority checks (fail on violation).
# -----------------------------------------------------------------------------

literal_source_scans="$(
  rg -n --no-heading --color=never 'include_str!\s*\(\s*".*\.rs"' "$CORE_ROOT" || true
)"
concat_source_scans="$(
  rg -n --no-heading --color=never 'include_str!\s*\(\s*concat!\([^)]*\.rs' "$CORE_ROOT" || true
)"
if [[ -n "$literal_source_scans" || -n "$concat_source_scans" ]]; then
  echo "[ERROR] include_str!-based source text scans are prohibited in $CORE_ROOT." >&2
  echo "[ERROR] Use structural/type/signature/invariant tests instead." >&2
  printf '%s\n%s\n' "$literal_source_scans" "$concat_source_scans" | sed '/^$/d' >&2
  status=1
fi

if [[ ! -f "$ROUTE_PLANNER_ROOT" ]]; then
  echo "[ERROR] Missing route planner root: $ROUTE_PLANNER_ROOT" >&2
  status=1
else
  sql_imports="$(run_rg 'db::sql::|sql::' "$ROUTE_PLANNER_ROOT" | strip_comment_only)"
  session_imports="$(run_rg 'db::session::|session::' "$ROUTE_PLANNER_ROOT" | strip_comment_only)"

  if [[ -n "$sql_imports" ]]; then
    echo "[ERROR] route planner root must not import sql-layer contracts directly." >&2
    echo "$sql_imports" >&2
    status=1
  fi

  if [[ -n "$session_imports" ]]; then
    echo "[ERROR] route planner root must not import session-layer contracts directly." >&2
    echo "$session_imports" >&2
    status=1
  fi

  route_families="$(
    { rg -o --no-heading --color=never '(access|direction|executor|query|sql|session|cursor|data|commit|index)::' \
      "$ROUTE_PLANNER_ROOT" || true; } \
      | sed 's/::$//' \
      | sort -u
  )"
  route_family_count="$(printf '%s\n' "$route_families" | awk 'NF { count += 1 } END { print count + 0 }')"

  if (( route_family_count > ALLOWED_TOP_LEVEL_DB_FAMILIES )); then
    echo "[ERROR] route planner root exceeded the allowed top-level db family ceiling." >&2
    echo "[ERROR] allowed: $ALLOWED_TOP_LEVEL_DB_FAMILIES" >&2
    echo "[ERROR] observed: $route_family_count" >&2
    printf '%s\n' "$route_families" | sed '/^$/d' | sed 's/^/[ERROR]   - /' >&2
    status=1
  fi
fi

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

KEY_COMPARATOR_PATTERN="as_bytes\\(\\)\\.cmp\\(|\\b(RawIndexStoreKey|IndexKey)\\b[^\\n]*\\.cmp\\("
key_comparator_leaks="$(
  run_rg "$KEY_COMPARATOR_PATTERN" "${ORDERING_AUDIT_DIRS[@]}" | strip_comment_only
)"
if [[ -n "$key_comparator_leaks" ]]; then
  echo "[ERROR] Index-key comparator logic must stay index-owned." >&2
  echo "$key_comparator_leaks" >&2
  status=1
fi

field_projection_files=()
for file in "${FIELD_PROJECTION_RUNTIME_FILES[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "[ERROR] Missing expected field-projection runtime file: $file" >&2
    status=1
    continue
  fi
  field_projection_files+=("$file")
done

FIELD_PROJECTION_PATTERN="\\.get_value\\(|\\bfn\\s+get_value\\s*\\("
if [[ ${#field_projection_files[@]} -gt 0 ]]; then
  field_projection_leaks="$(
    run_rg "$FIELD_PROJECTION_PATTERN" "${field_projection_files[@]}" | strip_comment_only
  )"
  if [[ -n "$field_projection_leaks" ]]; then
    echo "[ERROR] Runtime field projection must stay slot/index based." >&2
    echo "$field_projection_leaks" >&2
    status=1
  fi
fi

HAS_RESIDUAL_EXPR_THEN_PREDICATE_PATTERN="has_residual_filter_expr\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*has_residual_filter_predicate\\("
HAS_RESIDUAL_PREDICATE_THEN_EXPR_PATTERN="has_residual_filter_predicate\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*has_residual_filter_expr\\("
residual_filter_presence_leaks="$(
  {
    run_rg "$HAS_RESIDUAL_EXPR_THEN_PREDICATE_PATTERN" \
      "$DB_ROOT" \
      --glob '!crates/icydb-core/src/db/query/plan/access_plan.rs'
    run_rg "$HAS_RESIDUAL_PREDICATE_THEN_EXPR_PATTERN" \
      "$DB_ROOT" \
      --glob '!crates/icydb-core/src/db/query/plan/access_plan.rs'
  } | strip_comment_only
)"
if [[ -n "$residual_filter_presence_leaks" ]]; then
  echo "[ERROR] Residual-filter presence checks must use AccessPlannedQuery::has_any_residual_filter()." >&2
  echo "$residual_filter_presence_leaks" >&2
  status=1
fi

RESIDUAL_FILTER_EXPR_THEN_PREDICATE_PATTERN="residual_filter_expr\\(\\)\\.is_some\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*residual_filter_predicate\\(\\)\\.is_some\\("
RESIDUAL_FILTER_PREDICATE_THEN_EXPR_PATTERN="residual_filter_predicate\\(\\)\\.is_some\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*residual_filter_expr\\(\\)\\.is_some\\("
residual_filter_accessor_or_leaks="$(
  {
    run_rg "$RESIDUAL_FILTER_EXPR_THEN_PREDICATE_PATTERN" \
      "$DB_ROOT" \
      --glob '!crates/icydb-core/src/db/query/plan/access_plan.rs' \
      --glob '!crates/icydb-core/src/db/query/plan/semantics/logical.rs'
    run_rg "$RESIDUAL_FILTER_PREDICATE_THEN_EXPR_PATTERN" \
      "$DB_ROOT" \
      --glob '!crates/icydb-core/src/db/query/plan/access_plan.rs' \
      --glob '!crates/icydb-core/src/db/query/plan/semantics/logical.rs'
  } | strip_comment_only
)"
if [[ -n "$residual_filter_accessor_or_leaks" ]]; then
  echo "[ERROR] Residual-filter accessor presence checks must use AccessPlannedQuery::has_any_residual_filter()." >&2
  echo "$residual_filter_accessor_or_leaks" >&2
  status=1
fi

RESIDUAL_FILTER_FIELD_EXPR_THEN_PREDICATE_PATTERN="residual_filter_expr\\.is_some\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*residual_filter_predicate\\.is_some\\("
RESIDUAL_FILTER_FIELD_PREDICATE_THEN_EXPR_PATTERN="residual_filter_predicate\\.is_some\\(\\)[[:space:]]*\\|\\||\\|\\|[[:space:]][^\\n]*residual_filter_expr\\.is_some\\("
residual_filter_field_or_leaks="$(
  {
    run_rg "$RESIDUAL_FILTER_FIELD_EXPR_THEN_PREDICATE_PATTERN" "$DB_ROOT"
    run_rg "$RESIDUAL_FILTER_FIELD_PREDICATE_THEN_EXPR_PATTERN" "$DB_ROOT"
  } | strip_comment_only
)"
if [[ -n "$residual_filter_field_or_leaks" ]]; then
  echo "[ERROR] Residual-filter field presence must flow through ResidualFilterShape." >&2
  echo "$residual_filter_field_or_leaks" >&2
  status=1
fi

residual_filter_shape_rebuild_leaks="$(
  run_rg "ResidualFilterShape::from_presence\\(" "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/query/plan/access_plan.rs' \
    --glob '!crates/icydb-core/src/db/query/plan/semantics/logical.rs' \
    --glob '!crates/icydb-core/src/db/query/explain/execution.rs' \
    --glob '!**/tests/**' \
    | strip_comment_only
)"
if [[ -n "$residual_filter_shape_rebuild_leaks" ]]; then
  echo "[ERROR] Residual-filter shape construction must stay with planner/explain shape owners." >&2
  echo "$residual_filter_shape_rebuild_leaks" >&2
  status=1
fi

envelope_authority_leaks="$(
  run_rg "\\bfn\\s+(anchor_within_envelope|resume_bounds_from_refs|continuation_advanced)(\\s*<[^>]+>)?\\s*\\(" \
    "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/index/envelope/mod.rs' \
    | strip_comment_only
)"
if [[ -n "$envelope_authority_leaks" ]]; then
  echo "[ERROR] Envelope/continuation authority must remain centralized in db/index/envelope/mod.rs." >&2
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
  run_rg "\\bfn\\s+resume_bounds_from_refs(\\s*<[^>]+>)?\\s*\\(" "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/index/envelope/mod.rs' \
    | strip_comment_only
)"
if [[ -n "$continuation_rewrite_leaks" ]]; then
  echo "[ERROR] Continuation bound rewrite logic must stay in db/index/envelope/mod.rs." >&2
  echo "$continuation_rewrite_leaks" >&2
  status=1
fi

key_within_envelope_usage_leaks="$(
  run_rg "\\bkey_within_envelope\\b" "$DB_ROOT" \
    --glob '!crates/icydb-core/src/db/index/**' \
    --glob '!crates/icydb-core/src/db/cursor/**' \
    --glob '!crates/icydb-core/src/db/executor/mutation/commit_window.rs' \
    | strip_comment_only
)"
if [[ -n "$key_within_envelope_usage_leaks" ]]; then
  echo "[ERROR] key_within_envelope usage must remain index/cursor-owned with one executor mutation delegate." >&2
  echo "$key_within_envelope_usage_leaks" >&2
  status=1
fi

# 0.49 executor layer guardrails: prevent cross-layer import drift.
aggregate_scan_import_leaks="$(
  run_rg "db::executor::scan::" "crates/icydb-core/src/db/executor/aggregate" \
    | strip_comment_only
)"
if [[ -n "$aggregate_scan_import_leaks" ]]; then
  echo "[ERROR] Aggregate layer must not import scan layer internals." >&2
  echo "$aggregate_scan_import_leaks" >&2
  status=1
fi

terminal_scan_import_leaks="$(
  run_rg "db::executor::scan::" "crates/icydb-core/src/db/executor/terminal" \
    | strip_comment_only
)"
if [[ -n "$terminal_scan_import_leaks" ]]; then
  echo "[ERROR] Terminal layer must not import scan layer internals." >&2
  echo "$terminal_scan_import_leaks" >&2
  status=1
fi

terminal_planner_import_leaks="$(
  run_rg "db::query::plan::" "crates/icydb-core/src/db/executor/terminal" \
    | strip_comment_only
)"
if [[ -n "$terminal_planner_import_leaks" ]]; then
  echo "[ERROR] Terminal layer must not import planner contracts directly." >&2
  echo "$terminal_planner_import_leaks" >&2
  status=1
fi

planner_executor_import_leaks="$(
  run_rg "db::executor::" "crates/icydb-core/src/db/query/plan" \
    | strip_comment_only
)"
if [[ -n "$planner_executor_import_leaks" ]]; then
  echo "[ERROR] Planner contracts must not import executor-layer contracts." >&2
  echo "$planner_executor_import_leaks" >&2
  status=1
fi

pipeline_planner_import_leaks="$(
  run_rg "db::query::plan::" "crates/icydb-core/src/db/executor/pipeline" \
    --glob '!crates/icydb-core/src/db/executor/pipeline/contracts/**' \
    | strip_comment_only
)"
if [[ -n "$pipeline_planner_import_leaks" ]]; then
  echo "[ERROR] Pipeline layer must not import planner contracts directly." >&2
  echo "$pipeline_planner_import_leaks" >&2
  status=1
fi

scan_aggregate_import_leaks="$(
  run_rg "db::executor::aggregate::" "crates/icydb-core/src/db/executor/scan" \
    | strip_comment_only
)"
if [[ -n "$scan_aggregate_import_leaks" ]]; then
  echo "[ERROR] Scan layer must not import aggregate layer internals." >&2
  echo "$scan_aggregate_import_leaks" >&2
  status=1
fi

executor_shared_import_leaks="$(
  run_rg "executor::shared" "crates/icydb-core/src/db/executor" | strip_comment_only
)"
if [[ -n "$executor_shared_import_leaks" ]]; then
  echo "[ERROR] executor::shared namespace is deprecated; contracts must be owner-named." >&2
  echo "$executor_shared_import_leaks" >&2
  status=1
fi

if [[ -d "crates/icydb-core/src/db/executor/shared" ]]; then
  echo "[ERROR] executor/shared directory must not exist; re-home contracts to owner modules." >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Layer authority invariant checks failed." >&2
  exit 1
fi

echo "[OK] Layer authority invariants verified."
