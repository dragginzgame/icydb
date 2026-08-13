#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "[ERROR] ripgrep (rg) is required for generated endpoint invariant checks." >&2
  exit 1
fi

status=0

while IFS= read -r -d '' build_script; do
  if [[ "$(rg -c 'icydb::build::build_canister!\(' "$build_script")" != "1" ]]; then
    echo "[ERROR] $build_script must contain exactly one typed build_canister! invocation." >&2
    status=1
  fi
done < <(find canisters -name build.rs -print0)

for source in \
  canisters/audit/default_empty_metrics/src/lib.rs \
  canisters/audit/sql_perf/src/lib.rs \
  canisters/test/sql/src/lib.rs \
  canisters/demo/rpg/src/lib.rs
do
  if ! rg -q 'icydb::endpoints!\s*\{' "$source"; then
    echo "[ERROR] $source must own one explicit endpoints! block." >&2
    status=1
  fi
done

if rg -q '#\[.*ic_cdk::(query|update|init|post_upgrade)|#\[.*export_name|#\[.*no_mangle' \
  crates/icydb-model/src/build/actor/db \
  crates/icydb-model/src/build/actor/mod.rs
then
  echo "[ERROR] generated private actor capabilities must not contain IC export attributes; start! owns composed lifecycle exports." >&2
  status=1
fi

query_wrappers="$(rg -c '#\[\$crate::__reexports::ic_cdk::query\(name' crates/icydb/src/lib.rs)"
query_contexts="$(rg -c '\$crate::__macro::with_query_metrics_context' crates/icydb/src/lib.rs)"
if [[ "$query_wrappers" == "0" || "$query_wrappers" != "$query_contexts" ]]; then
  echo "[ERROR] every generated query wrapper must own exactly one synchronous metrics context." >&2
  status=1
fi

# Metrics handlers do not open database sessions. Every other generated
# handler must enter the default request scope immediately before dispatch so
# nested zero-argument db!() calls share one aggregate budget.
database_handler_count="$(
  rg 'crate::__icydb_generated::endpoint_handlers::' crates/icydb/src/lib.rs \
    | rg -vc 'crate::__icydb_generated::endpoint_handlers::metrics' \
    || true
)"
if [[ "$database_handler_count" == "0" ]]; then
  echo "[ERROR] generated database endpoint discovery matched no handlers." >&2
  status=1
fi
unscoped_database_handlers="$(
  awk '
    /crate::__icydb_generated::endpoint_handlers::/ &&
      $0 !~ /crate::__icydb_generated::endpoint_handlers::metrics/ {
        if (previous !~ /\$crate::db::with_request_execution\(\|\|/) {
          print FILENAME ":" FNR ":" $0
        }
      }
    { previous = $0 }
  ' crates/icydb/src/lib.rs
)"
if [[ -n "$unscoped_database_handlers" ]]; then
  echo "[ERROR] every generated database endpoint must enter one default request scope." >&2
  echo "$unscoped_database_handlers" >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  exit 1
fi

echo "[OK] Generated endpoint invariants verified."
