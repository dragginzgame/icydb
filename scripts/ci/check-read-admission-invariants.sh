#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "read-admission invariant checks"

status=0

DOC="docs/contracts/READ_ADMISSION.md"
ADMISSION="crates/icydb-core/src/db/query/admission.rs"
DIAGNOSTICS="crates/icydb-diagnostic-code/src/lib.rs"
TYPED_QUERY="crates/icydb/src/db/query/typed.rs"
PREPARED_QUERY="crates/icydb/src/db/session/prepared_query.rs"
GENERATED_SQL="crates/icydb-model/src/build/actor/db/sql.rs"
GENERATED_ENDPOINT="crates/icydb-model/src/build/actor/endpoint.rs"

extract_enum_variants() {
  local enum_name="$1"
  local source_file="$2"
  awk -v enum_name="$enum_name" '
    $0 ~ "enum " enum_name "[[:space:]]*\\{" { in_enum = 1; next }
    in_enum && /^}/ { exit }
    in_enum {
      line = $0
      sub(/\/\/.*/, "", line)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      sub(/,.*/, "", line)
      sub(/[[:space:]]*=.*/, "", line)
      if (line ~ /^[A-Z][A-Za-z0-9_]*$/) print line
    }
  ' "$source_file"
}

require_literal() {
  local file="$1"
  local description="$2"
  local literal="$3"
  if [[ ! -f "$file" ]] || ! rg -F --quiet "$literal" "$file"; then
    echo "[ERROR] Missing $description in $file: $literal" >&2
    status=1
  fi
}

# Numeric documentation data is compared with compiled owners in the focused
# documentation tests. Local links have a structural checker; do not assert
# prose, headings, code-example spelling, or numeric Rust source syntax here.

internal_variants="$(extract_enum_variants QueryAdmissionRejection "$ADMISSION")"
public_variants="$(extract_enum_variants QueryReadAdmissionCode "$DIAGNOSTICS")"
if [[ -z "$internal_variants" || -z "$public_variants" ]]; then
  echo "[ERROR] Read-admission rejection enums could not be inventoried." >&2
  status=1
fi

# Plan rejections are a subset: input admission can reject before a plan exists.
# Rust checks the exhaustive code() mapping; public diagnostics own wire order.
while IFS= read -r variant; do
  [[ -z "$variant" ]] && continue
  if ! rg -Fx --quiet "$variant" <<< "$public_variants"; then
    echo "[ERROR] Missing public read-admission counterpart: $variant" >&2
    status=1
  fi
done <<< "$internal_variants"

while IFS= read -r variant; do
  [[ -z "$variant" ]] && continue
  require_literal "$DOC" "public rejection documentation" "QueryReadAdmissionCode::$variant"
done <<< "$public_variants"

require_literal \
  "$TYPED_QUERY" \
  "typed live-page prepared-cursor handoff" \
  '.prepare_live_page_cursor(self.binding, self.request)'
require_literal \
  "$PREPARED_QUERY" \
  "identity-bound prepared live-page public admission" \
  '.execute_public_live_page_for_typed_binding('
require_literal \
  "$PREPARED_QUERY" \
  "accepted binding supplied to prepared live-page public admission" \
  'self.binding.inner(),'
require_literal \
  "$GENERATED_ENDPOINT" \
  "generated query controller gate" \
  'require_sql_controller'
require_literal \
  "$GENERATED_SQL" \
  "generated query trusted dispatch" \
  'execute_trusted_sql_query_dispatch'

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Read-admission invariants failed." >&2
  exit 1
fi

echo "[OK] Read-admission invariants verified."
