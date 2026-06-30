#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "read-admission invariant checks"

status=0

DOC="docs/contracts/READ_ADMISSION.md"
GENERATED_SQL="crates/icydb-build/src/db/sql.rs"
CONFIG_PARSE="crates/icydb-config/src/parse.rs"

if [[ ! -f "$DOC" ]]; then
  echo "[ERROR] Missing read-admission contract: $DOC" >&2
  status=1
else
  for required_phrase in \
    "Read Surface Inventory" \
    "\`PublicRead\`" \
    "\`AdminAdHoc\`" \
    "\`DiagnosticExplain\`" \
    "generated \`icydb_query\`" \
    "controller-gated" \
    "does not generate non-controller public SQL read endpoints" \
    "no \`sql.public_read\` key" \
    "execute_sql_query_with_read_admission_policy" \
    "execute_sql_query_with_perf_attribution"
  do
    if ! rg -F --quiet "$required_phrase" "$DOC"; then
      echo "[ERROR] Read-admission contract is missing required phrase: $required_phrase" >&2
      status=1
    fi
  done

  generated_query_names="$(
    rg -o --no-heading --color=never 'query\(name = "[^"]+"' crates/icydb-build/src \
      | sed 's/.*query(name = "//' \
      | sed 's/"$//' \
      | sort -u
  )"

  while IFS= read -r query_name; do
    [[ -z "$query_name" ]] && continue
    if ! rg -F --quiet "$query_name" "$DOC"; then
      echo "[ERROR] Generated query endpoint is missing from read-admission inventory: $query_name" >&2
      status=1
    fi
  done <<< "$generated_query_names"
fi

if [[ ! -f "$GENERATED_SQL" ]]; then
  echo "[ERROR] Missing generated SQL surface owner: $GENERATED_SQL" >&2
  status=1
else
  production_generated_sql="$(awk '/^#\[cfg\(test\)\]/{ exit } { print }' "$GENERATED_SQL")"

  if ! printf '%s\n' "$production_generated_sql" \
    | rg -F --quiet 'icydb_sql_surface_require_controller("query")'
  then
    echo "[ERROR] Generated icydb_query endpoint must remain controller-gated." >&2
    status=1
  fi

  if ! printf '%s\n' "$production_generated_sql" \
    | rg -F --quiet "execute_sql_query_with_perf_attribution"
  then
    echo "[ERROR] Generated icydb_query admin lane must keep using the trusted perf-attributed SQL helper." >&2
    status=1
  fi

  if printf '%s\n' "$production_generated_sql" \
    | rg -F --quiet 'ic_cdk::query(name = "icydb_public_query")'
  then
    echo "[ERROR] Generated SQL glue must not emit non-controller public query endpoints." >&2
    status=1
  fi

  if printf '%s\n' "$production_generated_sql" \
    | rg -F --quiet "QueryAdmissionPolicy::public_read"
  then
    echo "[ERROR] Generated SQL glue must not construct hidden public read policies." >&2
    status=1
  fi

  if printf '%s\n' "$production_generated_sql" \
    | rg -F --quiet "execute_sql_query_with_read_admission_policy"
  then
    echo "[ERROR] Generated SQL glue must not use the public read-admission seam; application-owned endpoints may use it directly." >&2
    status=1
  fi
fi

if [[ ! -f "$CONFIG_PARSE" ]]; then
  echo "[ERROR] Missing config parser owner: $CONFIG_PARSE" >&2
  status=1
else
  if rg -F --quiet "public_read" "$CONFIG_PARSE"
  then
    echo "[ERROR] icydb.toml must not accept generated sql.public_read config." >&2
    status=1
  fi

  if rg -F --quiet "RawCanisterSqlPublicRead" "$CONFIG_PARSE"
  then
    echo "[ERROR] Generated public SQL read config parser types must not exist." >&2
    status=1
  fi
fi

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Read-admission invariants failed." >&2
  exit 1
fi

echo "[OK] Read-admission invariants verified."
