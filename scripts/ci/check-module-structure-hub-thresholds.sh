#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

metrics_file="$(mktemp)"
trap 'rm -f "$metrics_file"' EXIT

bash scripts/audit/runtime_metrics.sh "$metrics_file"

status=0

check_module() {
  local module="$1"
  local max_loc="$2"
  local max_fanout="$3"
  local row

  row="$(awk -F '\t' -v module="$module" '$1 == module { print; found = 1 } END { if (!found) exit 1 }' "$metrics_file")" || {
    echo "[ERROR] Missing module metrics for ${module}." >&2
    status=1
    return
  }

  local loc fanout
  loc="$(awk -F '\t' '{ print $3 }' <<<"$row")"
  fanout="$(awk -F '\t' '{ print $10 }' <<<"$row")"

  if (( loc > max_loc )); then
    echo "[ERROR] ${module} loc ${loc} exceeds threshold ${max_loc}." >&2
    status=1
  fi

  if (( fanout > max_fanout )); then
    echo "[ERROR] ${module} fanout ${fanout} exceeds threshold ${max_fanout}." >&2
    status=1
  fi
}

check_module "db::session::sql::execute::write" 350 5
check_module "db::session::sql::execute::write::insert" 450 8
check_module "db::session::sql::execute::write::update" 250 7
check_module "db::session::sql::execute" 950 5
check_module "db::schema::reconcile" 650 8
check_module "db::schema::reconcile::sql_ddl" 450 5
check_module "db::schema::reconcile::sql_ddl::field_metadata" 550 5
check_module "db::schema::mutation" 1300 4
check_module "db::schema::mutation::field" 550 2
check_module "db::schema::mutation::index" 750 2
check_module "db::schema::mutation::runner" 850 3
check_module "db::sql::ddl" 750 2
check_module "db::sql::ddl::field" 750 3
check_module "db::sql::ddl::index" 750 2
check_module "db::relation" 500 6
check_module "db::relation::save_validate" 550 5
check_module "db::relation::reverse_index" 1100 6

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Module-structure hub thresholds failed." >&2
  exit 1
fi

echo "[OK] Module-structure hub thresholds verified."
