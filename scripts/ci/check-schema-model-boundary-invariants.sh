#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

SCHEMA_CARGO="crates/icydb-schema/Cargo.toml"
SCHEMA_ROOT="crates/icydb-schema/src"
MODEL_CARGO="crates/icydb-model/Cargo.toml"
MODEL_MACROS_CARGO="crates/icydb-model-macros/Cargo.toml"
SCHEMA_ONLY_FIXTURE_CARGO="testing/model-schema-only/Cargo.toml"
TYPED_ADAPTER_FIXTURE_CARGO="testing/model-typed-adapter/Cargo.toml"
CORE_CARGO="crates/icydb-core/Cargo.toml"
CORE_APPLICATION="crates/icydb-core/src/db/schema/application.rs"
CORE_SOURCE_BINDING="crates/icydb-core/src/db/schema/source_binding.rs"
CORE_ACCEPTED_BUNDLE="crates/icydb-core/src/db/schema/enum_catalog/publication.rs"

status=0

fail_with_matches() {
  local message="$1"
  local matches="$2"
  echo "[ERROR] $message" >&2
  printf '%s\n' "$matches" >&2
  status=1
}

schema_dependency_leaks="$(
  rg -n --no-heading --color=never \
    '^[[:space:]]*icydb-[a-z0-9-]+[[:space:]]*=' \
    "$SCHEMA_CARGO" || true
)"
if [[ -n "$schema_dependency_leaks" ]]; then
  fail_with_matches \
    "icydb-schema must remain a leaf with no dependency on another IcyDB package." \
    "$schema_dependency_leaks"
fi

schema_operation_leaks="$(
  rg -n --no-heading --color=never \
    '\b(now|generate|try_generate|normalize_for_storage|decode_row|encode_row)\s*\(' \
    "$SCHEMA_ROOT" || true
)"
if [[ -n "$schema_operation_leaks" ]]; then
  fail_with_matches \
    "icydb-schema scalar atoms must not own clocks, generation, normalization, or row codecs." \
    "$schema_operation_leaks"
fi

if ! rg -q --no-heading --color=never \
  'Scalar\(ScalarType\)' \
  "$SCHEMA_ROOT/fragment.rs"
then
  echo "[ERROR] proposal fields must retain exact scalar width and bound contracts." >&2
  status=1
fi

coarse_field_contract="$(
  rg -n --no-heading --color=never \
    'FieldType::Scalar\(ScalarKind|Scalar\(ScalarKind' \
    "$SCHEMA_ROOT" || true
)"
if [[ -n "$coarse_field_contract" ]]; then
  fail_with_matches \
    "coarse scalar capabilities must not replace exact proposal field contracts." \
    "$coarse_field_contract"
fi

for package_contract in \
  "$MODEL_CARGO:icydb-model" \
  "$MODEL_MACROS_CARGO:icydb-model-macros"
do
  manifest="${package_contract%%:*}"
  package="${package_contract#*:}"
  if ! rg -q --fixed-strings "name = \"$package\"" "$manifest"; then
    echo "[ERROR] final model package metadata must retain package identity: $package" >&2
    status=1
  fi
  if rg -q --no-heading --color=never '^publish[[:space:]]*=[[:space:]]*false$' "$manifest"; then
    echo "[ERROR] final model package must remain independently publishable: $package" >&2
    status=1
  fi
done

model_dependency_leaks="$(
  rg -n --no-heading --color=never \
    '^[[:space:]]*icydb[[:space:]]*=|^[[:space:]]*icydb-(cli|config|core|diagnostic-code|utils)[[:space:]]*=|package[[:space:]]*=[[:space:]]*"icydb"' \
    "$MODEL_CARGO" "$MODEL_MACROS_CARGO" || true
)"
if [[ -n "$model_dependency_leaks" ]]; then
  fail_with_matches \
    "final model packages may depend on icydb-schema and each other, never database/runtime packages." \
    "$model_dependency_leaks"
fi

for model_manifest in "$MODEL_CARGO" "$MODEL_MACROS_CARGO"; do
  if ! rg -q --no-heading --color=never \
    '^[[:space:]]*icydb-schema[[:space:]]*=' \
    "$model_manifest"
  then
    echo "[ERROR] final model packages must lower through icydb-schema: $model_manifest" >&2
    status=1
  fi
done

schema_only_runtime_leaks="$(
  rg -n --no-heading --color=never \
    'package[[:space:]]*=[[:space:]]*"icydb(-core)?"|^[[:space:]]*icydb(-core)?[[:space:]]*=' \
    "$SCHEMA_ONLY_FIXTURE_CARGO" || true
)"
if [[ -n "$schema_only_runtime_leaks" ]]; then
  fail_with_matches \
    "the schema-only model fixture must not depend on icydb or icydb-core." \
    "$schema_only_runtime_leaks"
fi

for required_typed_fixture_dependency in \
  'package = "icydb-model"' \
  'package = "icydb"'
do
  if ! rg -q --fixed-strings \
    "$required_typed_fixture_dependency" \
    "$TYPED_ADAPTER_FIXTURE_CARGO"
  then
    echo "[ERROR] typed-adapter fixture must exercise both renamed direct dependencies: $required_typed_fixture_dependency" >&2
    status=1
  fi
done

for required_application_owner in \
  'database_incarnation_id()' \
  'current_accepted_schema_root' \
  'stores.sort_unstable_by(|left, right| left.path.cmp(right.path))'
do
  if ! rg -q --fixed-strings "$required_application_owner" "$CORE_APPLICATION"; then
    echo "[ERROR] schema application targets must remain incarnation-, root-, and canonical-topology-owned: $required_application_owner" >&2
    status=1
  fi
done

for required_binding_owner in \
  'source_bindings: AcceptedSourceBindingCatalog' \
  'encode_accepted_source_bindings(' \
  'decode_accepted_source_bindings(' \
  'hash_len_prefixed(&mut hasher, &source_binding_bytes)?' \
  'writer.push_len_prefixed_bytes(&source_binding_bytes)?'
do
  if ! rg -q --fixed-strings "$required_binding_owner" "$CORE_ACCEPTED_BUNDLE"; then
    echo "[ERROR] accepted source bindings must remain bundle-, codec-, and fingerprint-owned: $required_binding_owner" >&2
    status=1
  fi
done

for sql_ddl_binding_owner in \
  crates/icydb-core/src/db/schema/sql_ddl.rs \
  crates/icydb-core/src/db/schema/sql_ddl/constraint.rs
do
  if ! rg -q --fixed-strings \
    '.with_sql_ddl_entity_transition(' \
    "$sql_ddl_binding_owner"
  then
    echo "[ERROR] SQL DDL candidates must evolve immutable source bindings with structural owners: $sql_ddl_binding_owner" >&2
    status=1
  fi
done

if ! rg -q --fixed-strings \
  'current.source_bindings().clone()' \
  crates/icydb-core/src/db/schema/constraint_activation_runner.rs
then
  echo "[ERROR] activation candidates must preserve immutable source bindings" >&2
  status=1
fi

for required_sql_ddl_binding_rule in \
  'with_sql_ddl_entity_transition(' \
  'field_lineage(' \
  'sql_ddl_source_key('
do
  if ! rg -q --fixed-strings \
    "$required_sql_ddl_binding_rule" \
    "$CORE_SOURCE_BINDING"
  then
    echo "[ERROR] SQL DDL source-binding ownership must remain catalog-native: $required_sql_ddl_binding_rule" >&2
    status=1
  fi
done

canonical_atoms=(
  Account Blob Date Decimal Duration Float32 Float64 IntBig NatBig Principal
  Subaccount Timestamp Ulid Unit
)
for atom in "${canonical_atoms[@]}"; do
  duplicate_definitions="$(
    rg -n --no-heading --color=never \
      "pub struct ${atom}([[:space:]]|\\(|\\{|<)" \
      crates/icydb-core/src crates/icydb/src || true
  )"
  if [[ -n "$duplicate_definitions" ]]; then
    fail_with_matches \
      "canonical scalar atom ${atom} must have one representation owner in icydb-schema." \
      "$duplicate_definitions"
  fi
done

if (( status != 0 )); then
  echo "[FAIL] Schema/model boundary invariants failed." >&2
  exit "$status"
fi

echo "[OK] Schema/model boundary invariants verified."
