#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "read-admission invariant checks"

status=0

DOC="docs/contracts/READ_ADMISSION.md"
GUIDE="docs/guides/read-intent.md"
POLICY="crates/icydb-core/src/db/query/admission/policy.rs"
ADMISSION="crates/icydb-core/src/db/query/admission.rs"
DIAGNOSTICS="crates/icydb-diagnostic-code/src/lib.rs"
TYPED_QUERY="crates/icydb/src/db/query/typed.rs"
FACADE_SQL="crates/icydb/src/db/session/sql.rs"
GENERATED_SQL="crates/icydb-model/src/build/actor/db/sql.rs"
CONFIG_PARSE="crates/icydb-config/src/parse.rs"

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

for section in \
  "# Read Admission" \
  "## Core Rule" \
  "## Read Surface Inventory" \
  "## Which API should I use?" \
  "## Generated SQL Query Surface" \
  "## Public Endpoint Guidance" \
  "## Common Rejections And Fixes" \
  "## Regression Guard"
do
  require_literal "$DOC" "read-admission section" "$section"
done

for literal in \
  'maximum returned rows: 100' \
  '1024 terms and 64 KiB' \
  '100 groups, 64 KiB per group, and 1024 distinct' \
  '`execute_public_dynamic_query`' \
  '`execute_trusted_dynamic_query`' \
  '`execute_trusted_sql_query`' \
  'generated `icydb_query`' \
  '`DiagnosticExplain`'
do
  require_literal "$DOC" "read-admission contract fact" "$literal"
done

for literal in \
  'const DEFAULT_BOUNDED_READ_MAX_ROWS: u32 = 100;' \
  'const DEFAULT_BOUNDED_READ_MAX_GROUPS: u32 = 100;' \
  'const DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES: u32 = 64 * 1024;' \
  'const DEFAULT_BOUNDED_READ_MAX_DISTINCT_ENTRIES: u32 = 1024;' \
  'const DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_TERMS: u32 = 1024;' \
  'const DEFAULT_BOUNDED_READ_MAX_PRIMARY_KEY_INPUT_BYTES: u32 = 64 * 1024;'
do
  require_literal "$POLICY" "read-admission budget authority" "$literal"
done

internal_variants="$(extract_enum_variants QueryAdmissionRejection "$ADMISSION")"
public_variants="$(extract_enum_variants QueryReadAdmissionCode "$DIAGNOSTICS")"
if [[ -z "$internal_variants" || "$internal_variants" != "$public_variants" ]]; then
  echo "[ERROR] Internal and public read-admission rejection variants diverged." >&2
  status=1
fi

while IFS= read -r variant; do
  [[ -z "$variant" ]] && continue
  require_literal "$DOC" "public rejection documentation" "QueryReadAdmissionCode::$variant"
done <<< "$public_variants"

require_literal \
  "$TYPED_QUERY" \
  "typed query public-admission handoff" \
  '.execute_public_dynamic_query(&self.request)'
require_literal \
  "$FACADE_SQL" \
  "trusted SQL caller-control warning" \
  'caller-controlled SQL public-safe'
require_literal \
  "$GENERATED_SQL" \
  "generated query controller gate" \
  'icydb_sql_surface_require_controller("query")'
require_literal \
  "$GENERATED_SQL" \
  "generated query trusted dispatch" \
  'execute_trusted_sql_query_with_perf_attribution'
require_literal \
  "$GUIDE" \
  "typed public endpoint guidance" \
  '.query::<User>()?'

if rg -F --quiet "public_read" "$CONFIG_PARSE"; then
  echo "[ERROR] icydb.toml must not expose generated public SQL-read policy." >&2
  status=1
fi

if [[ $status -ne 0 ]]; then
  echo "[FAIL] Read-admission invariants failed." >&2
  exit 1
fi

echo "[OK] Read-admission invariants verified."
