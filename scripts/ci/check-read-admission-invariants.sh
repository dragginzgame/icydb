#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "read-admission invariant checks"

status=0

DOC="docs/contracts/READ_ADMISSION.md"
READ_INTENT_GUIDE="docs/guides/read-intent.md"
README_DOC="README.md"
INSTALLING_DOC="INSTALLING.md"
FOUNDATIONS_DOC="docs/FOUNDATIONS.md"
QUERY_CONTRACT_DOC="docs/contracts/QUERY_CONTRACT.md"
QUERY_PRACTICE_DOC="docs/contracts/QUERY_PRACTICE.md"
SQL_SUBSET_DOC="docs/contracts/SQL_SUBSET.md"
GENERATED_SQL="crates/icydb-build/src/db/sql.rs"
CONFIG_PARSE="crates/icydb-config/src/parse.rs"
PUBLIC_FACADE="crates/icydb/src"
PUBLIC_CRATE_LIB="crates/icydb/src/lib.rs"
PUBLIC_FACADE_SESSION="crates/icydb/src/db/session/mod.rs"
PUBLIC_FACADE_LOAD="crates/icydb/src/db/session/load.rs"
PUBLIC_FACADE_SESSION_MACROS="crates/icydb/src/db/session/macros.rs"
ADMISSION_SOURCE="crates/icydb-core/src/db/query/admission.rs"
READ_INTENT_SOURCE="crates/icydb-core/src/db/query/read_intent.rs"
DIAGNOSTIC_CODES="crates/icydb-diagnostic-code/src/lib.rs"

extract_rust_enum_variants() {
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

require_file_literal() {
  local owner="$1"
  local label="$2"
  local literal="$3"

  if ! rg -F --quiet "$literal" "$owner"; then
    echo "[ERROR] $owner is missing required $label: $literal" >&2
    status=1
  fi
}

require_file_pattern() {
  local owner="$1"
  local label="$2"
  local pattern="$3"

  if ! rg --quiet "$pattern" "$owner"; then
    echo "[ERROR] $owner is missing required concept: $label" >&2
    status=1
  fi
}

if [[ ! -f "$DOC" ]]; then
  echo "[ERROR] Missing read-admission contract: $DOC" >&2
  status=1
else
  for required_contract_section in \
    "^## Core Rule$" \
    "^## Read Surface Inventory$" \
    "^## Which API should I use\\?$" \
    "^## Generated SQL Query Surface$" \
    "^## Public Endpoint Guidance$" \
    "^## Common Rejections And Fixes$" \
    "^## Regression Guard$"
  do
    require_file_pattern "$DOC" "section anchor $required_contract_section" "$required_contract_section"
  done

  for required_contract_token in \
    "\`PublicRead\`" \
    "\`AdminAdHoc\`" \
    "\`DiagnosticExplain\`" \
    "generated \`icydb_query\`" \
    "hidden prebuilt-query execution" \
    "\`trusted_read_unchecked()\` fluent lane" \
    "\`trusted_read_unchecked()\`" \
    "\`execute_sql_query\`" \
    "execute_sql_query_with_perf_attribution" \
    "docs/guides/read-intent.md" \
    "\`page(limit)\`" \
    "\`next_page(limit, cursor)\`" \
    "\`collect_complete()\`" \
    "\`count_exact()\`" \
    "\`sum_exact(field)\`" \
    "\`explain_count_exact()\`" \
    "\`explain_sum_exact(field)\`" \
    "\`execute().into_grouped()\`"
  do
    require_file_literal "$DOC" "API/lane token" "$required_contract_token"
  done

  for required_budget_literal in \
    "maximum returned rows: 100" \
    "maximum plan-level response bytes: 128 KiB" \
    "100 groups, 64 KiB per group, and 1024 distinct entries"
  do
    require_file_literal "$DOC" "budget literal" "$required_budget_literal"
  done

  declare -A required_contract_concepts=(
    ["generated icydb_query remains controller gated"]="generated .*icydb_query.*controller-gated"
    ["caller-controlled SQL is not public-safe"]="caller-controlled SQL.*execute_sql_query"
    ["generated SQL has no public-read config"]="sql\\.public_read"
    ["generated non-controller public SQL endpoints remain forbidden"]="non-controller.*generated SQL query endpoint"
    ["read-intent guidance is discoverable"]="read-intent\\.md"
    ["public list guidance uses cursor page terminals"]="public list endpoints use .*page\\(limit\\).*next_page\\(limit, cursor\\)"
    ["complete-result guidance uses collect_complete"]="complete-result endpoints use .*collect_complete"
    ["exact aggregate guidance uses exact helpers"]="exact aggregate.*count_exact"
  )

  for required_contract_concept in "${!required_contract_concepts[@]}"; do
    require_file_pattern \
      "$DOC" \
      "$required_contract_concept" \
      "${required_contract_concepts[$required_contract_concept]}"
  done

  if [[ ! -f "$DIAGNOSTIC_CODES" ]]; then
    echo "[ERROR] Missing diagnostic code source: $DIAGNOSTIC_CODES" >&2
    status=1
  else
    found_rejection_code=0
    while IFS= read -r rejection_variant; do
      found_rejection_code=1
      required_rejection_code="QueryReadAdmissionCode::$rejection_variant"
      if ! rg -F --quiet "$required_rejection_code" "$DOC"; then
        echo "[ERROR] Read-admission common rejection table is missing diagnostic detail: $required_rejection_code" >&2
        status=1
      fi
    done < <(extract_rust_enum_variants "QueryReadAdmissionCode" "$DIAGNOSTIC_CODES")
    if [[ "$found_rejection_code" -eq 0 ]]; then
      echo "[ERROR] No QueryReadAdmissionCode variants discovered in: $DIAGNOSTIC_CODES" >&2
      status=1
    fi
  fi

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

if [[ ! -f "$READ_INTENT_GUIDE" ]]; then
  echo "[ERROR] Missing read-intent guide: $READ_INTENT_GUIDE" >&2
  status=1
else
  for required_read_intent_section in \
    "^# Read Intent Guide$" \
    "^## Current Map$" \
    "^## When Admission Rejects A Read$" \
    "^## Migration Recipes$" \
    "^## Generated SQL Boundary$" \
    "^## Public Endpoint Templates$" \
    "^## Exact Lookup$" \
    "^## Existence$" \
    "^## Exact Aggregates$" \
    "^## Public Pages$" \
    "^## Complete Small Sets$" \
    "^## Trusted Reads$" \
    "^## Migration Checklist$"
  do
    if ! rg --quiet "$required_read_intent_section" "$READ_INTENT_GUIDE"; then
      echo "[ERROR] Read-intent guide is missing required section anchor: $required_read_intent_section" >&2
      status=1
    fi
  done

  for required_read_intent_token in \
    "PublicQueryRequiresLimit" \
    "partial_window(...)" \
    "page(limit)" \
    "next_page(limit, cursor)" \
    "AdminBatchRequest" \
    "collect_complete()" \
    "count_exact()" \
    "sum_exact(field)" \
    "explain_count_exact()" \
    "explain_sum_exact(field)" \
    "ReadIntentKind::CompleteSmallSet" \
    "icydb_query" \
    "Endpoint review checklist:" \
    "partial row window"
  do
    if ! rg -F --quiet "$required_read_intent_token" "$READ_INTENT_GUIDE"; then
      echo "[ERROR] Read-intent guide is missing required concept/API token: $required_read_intent_token" >&2
      status=1
    fi
  done

  declare -A required_read_intent_concepts=(
    ["partial_window is the partial-row-window primitive"]="partial row window: use .*partial_window\\(N\\)\\.execute_rows\\(\\)"
    ["partial_window migration is not mechanical"]="mechanically replace .*partial_window\\(N\\)\\.execute_rows\\(\\)"
    ["generated SQL is not a public endpoint substitute"]="Generated SQL endpoints.*public read endpoints"
    ["generated SQL wrapper warning"]="Do not expose .*icydb_query"
  )

  for required_read_intent_concept in "${!required_read_intent_concepts[@]}"; do
    required_read_intent_pattern="${required_read_intent_concepts[$required_read_intent_concept]}"
    if ! rg --quiet "$required_read_intent_pattern" "$READ_INTENT_GUIDE"; then
      echo "[ERROR] Read-intent guide is missing required concept: $required_read_intent_concept" >&2
      status=1
    fi
  done
fi

declare -A required_read_admission_links=(
  ["$README_DOC"]="[docs/contracts/READ_ADMISSION.md](docs/contracts/READ_ADMISSION.md)"
  ["$INSTALLING_DOC"]="[docs/contracts/READ_ADMISSION.md](docs/contracts/READ_ADMISSION.md)"
  ["$FOUNDATIONS_DOC"]="docs/contracts/READ_ADMISSION.md"
  ["$QUERY_CONTRACT_DOC"]="docs/contracts/READ_ADMISSION.md"
  ["$QUERY_PRACTICE_DOC"]="docs/contracts/READ_ADMISSION.md"
  ["$SQL_SUBSET_DOC"]="docs/contracts/READ_ADMISSION.md"
  ["$PUBLIC_CRATE_LIB"]="docs/contracts/READ_ADMISSION.md"
)

for link_owner in "${!required_read_admission_links[@]}"; do
  required_link="${required_read_admission_links[$link_owner]}"
  if [[ ! -f "$link_owner" ]]; then
    echo "[ERROR] Missing read-admission discovery document: $link_owner" >&2
    status=1
    continue
  fi
  if ! rg -F --quiet "$required_link" "$link_owner"; then
    echo "[ERROR] Read-admission contract is not discoverable from $link_owner: $required_link" >&2
    status=1
  fi
done

require_file_pattern \
  "$README_DOC" \
  "typed/fluent reads mention bounded admission" \
  "typed/fluent reads.*bounded"

require_file_pattern \
  "$README_DOC" \
  "execute_sql_query is documented as trusted/admin" \
  "execute_sql_query.*trusted/admin"

require_file_pattern \
  "$INSTALLING_DOC" \
  "readonly SQL is generated controller-gated admin surface" \
  "Readonly SQL.*controller-gated.*admin"

require_file_literal "$PUBLIC_CRATE_LIB" "bounded admission wording" "read-admission gate"
require_file_pattern \
  "$PUBLIC_CRATE_LIB" \
  "public crate docs mention bounded read admission" \
  "bounded"

require_file_literal "$PUBLIC_CRATE_LIB" "read-intent guide link" "docs/guides/read-intent.md"

require_file_pattern \
  "$PUBLIC_CRATE_LIB" \
  "generated SQL remains controller-gated admin surface" \
  "Generated SQL endpoints.*controller-gated.*admin"

if [[ ! -f "$ADMISSION_SOURCE" ]]; then
  echo "[ERROR] Missing read-admission source owner: $ADMISSION_SOURCE" >&2
  status=1
else
  for required_source_constant in \
    "const DEFAULT_BOUNDED_READ_MAX_ROWS: u32 = 100;" \
    "const DEFAULT_BOUNDED_READ_RESPONSE_BYTES: u32 = 128 * 1024;" \
    "const DEFAULT_BOUNDED_READ_MAX_GROUPS: u32 = 100;" \
    "const DEFAULT_BOUNDED_READ_MAX_GROUP_BYTES: u32 = 64 * 1024;" \
    "const DEFAULT_BOUNDED_READ_MAX_DISTINCT_ENTRIES: u32 = 1024;"
  do
    if ! rg -F --quiet "$required_source_constant" "$ADMISSION_SOURCE"; then
      echo "[ERROR] Default read-admission budget changed without updating the invariant contract: $required_source_constant" >&2
      status=1
    fi
  done

  if [[ -f "$DIAGNOSTIC_CODES" ]]; then
    public_rejection_variants="$(
      extract_rust_enum_variants "QueryReadAdmissionCode" "$DIAGNOSTIC_CODES"
    )"
    internal_rejection_variants="$(
      extract_rust_enum_variants "QueryAdmissionRejection" "$ADMISSION_SOURCE"
    )"
    if [[ -z "$public_rejection_variants" ]]; then
      echo "[ERROR] No public QueryReadAdmissionCode variants discovered in: $DIAGNOSTIC_CODES" >&2
      status=1
    elif [[ -z "$internal_rejection_variants" ]]; then
      echo "[ERROR] No internal QueryAdmissionRejection variants discovered in: $ADMISSION_SOURCE" >&2
      status=1
    elif [[ "$internal_rejection_variants" != "$public_rejection_variants" ]]; then
      echo "[ERROR] Internal QueryAdmissionRejection variants must match public QueryReadAdmissionCode variants one-for-one." >&2
      echo "[ERROR] Internal variants:" >&2
      printf '%s\n' "$internal_rejection_variants" >&2
      echo "[ERROR] Public variants:" >&2
      printf '%s\n' "$public_rejection_variants" >&2
      status=1
    fi
  fi
fi

if [[ ! -f "$READ_INTENT_SOURCE" ]]; then
  echo "[ERROR] Missing read-intent cap authority: $READ_INTENT_SOURCE" >&2
  status=1
else
  for required_read_intent_constant in \
    "const PUBLIC_PAGE_DEFAULT_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;" \
    "const PUBLIC_PAGE_MAX_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;" \
    "const PUBLIC_PAGE_MAX_RESPONSE_BYTES: u32 =" \
    "const COMPLETE_SMALL_MAX_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;" \
    "const COMPLETE_SMALL_LOOKAHEAD_ROWS: u32 = 1;" \
    "const COMPLETE_SMALL_EXECUTION_LIMIT: u32 =" \
    "const ADMIN_BATCH_ROWS: u32 = DEFAULT_BOUNDED_READ_MAX_ROWS;"
  do
    if ! rg -F --quiet "$required_read_intent_constant" "$READ_INTENT_SOURCE"; then
      echo "[ERROR] Read-intent cap authority drifted or split: $required_read_intent_constant" >&2
      status=1
    fi
  done

  for forbidden_read_intent_pattern in \
    "ReadPolicy" \
    "PolicyBuilder" \
    "with_max_rows" \
    "with_max_response_bytes" \
    "custom_policy"
  do
    if rg -F --quiet "$forbidden_read_intent_pattern" "$READ_INTENT_SOURCE"; then
      echo "[ERROR] Read-intent source must not introduce public custom policy surface: $forbidden_read_intent_pattern" >&2
      status=1
    fi
  done
fi

high_raw_limit_hits="$(
  rg -n --color=never '\.limit\((1000|1_000|10000|10_000)\)' \
    README.md INSTALLING.md docs/contracts docs/guides crates/icydb/src crates/icydb-core/src canisters \
    2>/dev/null \
    | rg -v '^docs/contracts/READ_ADMISSION\.md:' || true
)"
if [[ -n "$high_raw_limit_hits" ]]; then
  echo "[ERROR] Raw high-limit examples must not appear as recommended docs/API patterns." >&2
  echo "[ERROR] Use page(limit) / next_page(limit, cursor), collect_complete(), exact aggregates, or mark the example as a rejection in READ_ADMISSION.md." >&2
  printf '%s\n' "$high_raw_limit_hits" >&2
  status=1
fi

if [[ ! -d "$PUBLIC_FACADE" ]]; then
  echo "[ERROR] Missing public facade source directory: $PUBLIC_FACADE" >&2
  status=1
else
  for forbidden_public_facade_pattern in \
    "execute_query_with_policy" \
    "execute_with_policy" \
    "with_query_policy" \
    "execute_query_with_read_admission_policy" \
    "execute_sql_query_with_read_admission_policy" \
    "QueryAdmissionPolicy" \
    "GroupedAdmissionPolicy" \
    "public_custom" \
    "public_read_policy"
  do
    if rg -F --quiet "$forbidden_public_facade_pattern" "$PUBLIC_FACADE"; then
      echo "[ERROR] Public facade must not reintroduce custom read-policy API: $forbidden_public_facade_pattern" >&2
      status=1
    fi
  done

  declare -A required_public_facade_tokens=(
    ["$PUBLIC_FACADE_SESSION"]="caller-controlled SQL"
    ["$PUBLIC_FACADE_SESSION_MACROS"]="QueryResponse::Grouped"
    ["$PUBLIC_FACADE_LOAD"]="execute().into_grouped()"
  )

  for public_facade_file in "${!required_public_facade_tokens[@]}"; do
    required_token="${required_public_facade_tokens[$public_facade_file]}"
    if [[ ! -f "$public_facade_file" ]]; then
      echo "[ERROR] Missing public facade read-admission source: $public_facade_file" >&2
      status=1
      continue
    fi
    require_file_literal "$public_facade_file" "read-admission doc token" "$required_token"
  done

  require_file_pattern \
    "$PUBLIC_FACADE_LOAD" \
    "cursor pagination mentions default bounded admission" \
    "Cursor pagination runs through the default bounded read-admission lane"

  require_file_pattern \
    "$PUBLIC_FACADE_SESSION" \
    "SQL helper warns about caller-controlled SQL" \
    "caller-controlled SQL.*public-safe"

  require_file_pattern \
    "$PUBLIC_FACADE_SESSION" \
    "SQL attribution helper keeps generated controller-gated lane wording" \
    "generated controller-gated SQL surfaces"

  require_file_pattern \
    "$PUBLIC_FACADE_SESSION" \
    "direct query execution is hidden from normal endpoint API" \
    "Normal endpoint code should prefer .*semantic"
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
    echo "[ERROR] Generated SQL glue must not use removed custom read-policy helpers." >&2
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
