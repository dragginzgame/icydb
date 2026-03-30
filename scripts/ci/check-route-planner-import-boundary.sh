#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

ROUTE_PLANNER_ROOT="crates/icydb-core/src/db/executor/route/planner/mod.rs"
ALLOWED_TOP_LEVEL_DB_FAMILIES=3

if [[ ! -f "$ROUTE_PLANNER_ROOT" ]]; then
  echo "[ERROR] Missing route planner root: $ROUTE_PLANNER_ROOT" >&2
  exit 1
fi

# Keep the route planner root fenced away from frontend/session concerns.
sql_imports="$(
  rg -n --no-heading --color=never 'db::sql::|sql::' "$ROUTE_PLANNER_ROOT" || true
)"
session_imports="$(
  rg -n --no-heading --color=never 'db::session::|session::' "$ROUTE_PLANNER_ROOT" || true
)"

if [[ -n "$sql_imports" ]]; then
  echo "[ERROR] route planner root must not import sql-layer contracts directly." >&2
  echo "$sql_imports" >&2
  exit 1
fi

if [[ -n "$session_imports" ]]; then
  echo "[ERROR] route planner root must not import session-layer contracts directly." >&2
  echo "$session_imports" >&2
  exit 1
fi

# Track distinct top-level db families referenced by the root import surface.
# This is intentionally coarse: it keeps the root from accreting new families
# even when the exact imported items move around inside those families.
families="$(
  rg -o --no-heading --color=never '(access|direction|executor|query|sql|session|cursor|data|commit|index)::' \
    "$ROUTE_PLANNER_ROOT" \
    | sed 's/::$//' \
    | sort -u
)"
family_count="$(printf '%s\n' "$families" | awk 'NF { count += 1 } END { print count + 0 }')"

echo "Route planner root import families: $family_count"
printf '%s\n' "$families" | sed '/^$/d' | sed 's/^/  - /'

if (( family_count > ALLOWED_TOP_LEVEL_DB_FAMILIES )); then
  echo "[ERROR] route planner root exceeded the allowed top-level db family ceiling." >&2
  echo "[ERROR] allowed: $ALLOWED_TOP_LEVEL_DB_FAMILIES" >&2
  echo "[ERROR] observed: $family_count" >&2
  exit 1
fi

echo "[OK] Route planner import boundary is within the configured ceiling."
