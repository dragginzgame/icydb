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

KEY_COMPARATOR_PATTERN="as_bytes\\(\\)\\.cmp\\(|\\b(RawIndexStoreKey|IndexKey)\\b[^\\n]*\\.cmp\\("
key_comparator_leaks="$(
  run_rg "$KEY_COMPARATOR_PATTERN" "${ORDERING_AUDIT_DIRS[@]}" | strip_comment_only
)"
if [[ -n "$key_comparator_leaks" ]]; then
  echo "[ERROR] Index-key comparator logic must stay index-owned." >&2
  echo "$key_comparator_leaks" >&2
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
