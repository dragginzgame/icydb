#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

filter_out_allowed_files() {
  local file
  local patterns=()
  for file in "$@"; do
    patterns+=(-e "^${file//\//\\/}:")
  done

  if [[ ${#patterns[@]} -eq 0 ]]; then
    cat
    return
  fi

  rg -v --no-heading "${patterns[@]}" || true
}

status=0

# -----------------------------------------------------------------------------
# A. Execution-layer semantic revalidation must not spread beyond the current
# execution owner quarantine. This is intentionally narrow: grouped/runtime
# packaging is allowed in execution, but semantic shape checks must not start
# diffusing into sibling execution files.
# -----------------------------------------------------------------------------

EXECUTION_SQL_ROOT="crates/icydb-core/src/db/session/sql"
EXECUTION_SEMANTIC_ALLOWED=(
  "crates/icydb-core/src/db/session/sql/execute/mod.rs"
)
EXECUTION_SEMANTIC_PATTERN="group_by\\.|having\\.|SqlSelectItem::Aggregate|projection_aggregates|grouped_projection_aggregates|validate_sql_insert_(required_fields|value_tuple_lengths|selected_rows)\\("

execution_semantic_leaks="$(
  run_rg "$EXECUTION_SEMANTIC_PATTERN" "$EXECUTION_SQL_ROOT" \
    | strip_comment_only \
    | filter_out_allowed_files "${EXECUTION_SEMANTIC_ALLOWED[@]}"
)"
if [[ -n "$execution_semantic_leaks" ]]; then
  echo "[ERROR] SQL execution semantic revalidation spread beyond the sanctioned execution owner." >&2
  echo "[ERROR] Keep grouped/aggregate/HAVING semantic checks in lowering, with the current INSERT SELECT quarantine only in execute/mod.rs." >&2
  echo "$execution_semantic_leaks" >&2
  status=1
fi

# -----------------------------------------------------------------------------
# B. Executor and terminal layers must not grow SQL-facing output shaping.
# -----------------------------------------------------------------------------

EXECUTOR_ROOT="crates/icydb-core/src/db/executor"
EXECUTOR_SQL_IMPORT_PATTERN="db::session::sql|db::sql::(parser|lowering)"
executor_sql_import_leaks="$(
  run_rg "$EXECUTOR_SQL_IMPORT_PATTERN" "$EXECUTOR_ROOT" | strip_comment_only
)"
if [[ -n "$executor_sql_import_leaks" ]]; then
  echo "[ERROR] Executor must not import SQL parser/lowering/session internals." >&2
  echo "$executor_sql_import_leaks" >&2
  status=1
fi

EXECUTOR_SQL_LABEL_PATTERN="render_text_projection_expr_sql_label|projection_field_label\\(|\\.sql_label\\("

executor_sql_label_leaks="$(
  run_rg "$EXECUTOR_SQL_LABEL_PATTERN" "$EXECUTOR_ROOT" \
    | strip_comment_only
)"
if [[ -n "$executor_sql_label_leaks" ]]; then
  echo "[ERROR] SQL-facing alias/label rendering leaked into executor ownership." >&2
  echo "[ERROR] Keep SQL output naming and text-function label rendering under db/session/sql/projection only." >&2
  echo "$executor_sql_label_leaks" >&2
  status=1
fi

TERMINAL_ROOT="crates/icydb-core/src/db/executor/terminal"
if [[ -d "$TERMINAL_ROOT" ]]; then
  terminal_sql_output_leaks="$(
    run_rg "alias|label|render_text_projection_expr_sql_label|\\.sql_label\\(" "$TERMINAL_ROOT" \
      | strip_comment_only
  )"
  if [[ -n "$terminal_sql_output_leaks" ]]; then
    echo "[ERROR] Terminal layer must not own SQL-facing alias/label formatting." >&2
    echo "$terminal_sql_output_leaks" >&2
    status=1
  fi
fi

# -----------------------------------------------------------------------------
# C. Lowering must not reparse tokens or own clause-order parsing logic.
# -----------------------------------------------------------------------------

LOWERING_ROOT="crates/icydb-core/src/db/sql/lowering"
LOWERING_REPARSE_PATTERN="TokenKind|\\bKeyword\\b|eat_keyword\\(|peek_keyword\\(|eat_comma\\(|peek_lparen\\(|trailing_clause_order_error|peek_unsupported_feature"

lowering_reparse_leaks="$(
  run_rg "$LOWERING_REPARSE_PATTERN" "$LOWERING_ROOT" | strip_comment_only
)"
if [[ -n "$lowering_reparse_leaks" ]]; then
  echo "[ERROR] SQL lowering must not reparse tokens or re-own clause-order parsing." >&2
  echo "$lowering_reparse_leaks" >&2
  status=1
fi

# -----------------------------------------------------------------------------
# D. Parser and SELECT-lowering helper seams must stay inside the SQL frontend.
# -----------------------------------------------------------------------------

PARSER_ROOT="crates/icydb-core/src/db/sql/parser"
parser_visibility_leaks="$(
  run_rg "pub\\(in crate::db\\)" "$PARSER_ROOT" | strip_comment_only
)"
if [[ -n "$parser_visibility_leaks" ]]; then
  echo "[ERROR] SQL parser helper visibility must not widen past the SQL frontend boundary." >&2
  echo "$parser_visibility_leaks" >&2
  status=1
fi

SELECT_LOWERING_ROOT="crates/icydb-core/src/db/sql/lowering/select"
SELECT_BINDING_FILE="$SELECT_LOWERING_ROOT/binding.rs"
select_canonicalizer_export_leaks="$(
  run_rg "pub\\(in crate::db\\).*canonicalize_sql_(predicate|filter_expr)_for_schema" \
    "$SELECT_LOWERING_ROOT" \
    | strip_comment_only
)"
if [[ -n "$select_canonicalizer_export_leaks" ]]; then
  echo "[ERROR] SQL SELECT strict-literal canonicalizers must remain SELECT-lowering private." >&2
  echo "$select_canonicalizer_export_leaks" >&2
  status=1
fi

for required_canonicalizer in \
  "pub(super) fn canonicalize_sql_predicate_for_schema" \
  "pub(super) fn canonicalize_sql_filter_expr_for_schema"
do
  if ! rg -F -n --no-heading --color=never "$required_canonicalizer" \
    "$SELECT_BINDING_FILE" >/dev/null
  then
    echo "[ERROR] SQL SELECT strict-literal canonicalizer visibility drifted from pub(super)." >&2
    echo "[ERROR] Missing required shape in $SELECT_BINDING_FILE: $required_canonicalizer" >&2
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "[FAIL] SQL branch ownership invariants failed." >&2
  exit 1
fi

echo "[OK] SQL branch ownership invariants verified."
