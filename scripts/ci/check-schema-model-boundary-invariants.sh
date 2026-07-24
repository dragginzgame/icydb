#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

SCHEMA_CARGO="crates/icydb-schema/Cargo.toml"
SCHEMA_ROOT="crates/icydb-schema/src"
LEGACY_CARGO="crates/icydb-model-legacy/Cargo.toml"
CORE_CARGO="crates/icydb-core/Cargo.toml"
CORE_APPLICATION="crates/icydb-core/src/db/schema/application.rs"

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
    '^[[:space:]]*icydb-(core|model|model-legacy|schema-derive|derive|build|config)[[:space:]]*=' \
    "$SCHEMA_CARGO" || true
)"
if [[ -n "$schema_dependency_leaks" ]]; then
  fail_with_matches \
    "icydb-schema must remain a leaf with no dependency on another IcyDB package." \
    "$schema_dependency_leaks"
fi

schema_authority_leaks="$(
  rg -n --no-heading --color=never \
    '\b(EntityModel|EntityRuntimeHooks|SchemaInfo|AcceptedEntity|AcceptedRecordType|FieldId|SchemaIndexId|RelationId|RowLayoutVersion|RuntimeValue|StoreHandle)\b|icydb_core|icydb_model' \
    "$SCHEMA_ROOT" || true
)"
if [[ -n "$schema_authority_leaks" ]]; then
  fail_with_matches \
    "icydb-schema must not import or define model, accepted-runtime, or storage authority." \
    "$schema_authority_leaks"
fi

schema_operation_leaks="$(
  rg -n --no-heading --color=never \
    '\b(now|generate|try_generate|sanitize|normalize_for_storage|decode_row|encode_row)\s*\(' \
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

if ! rg -q --no-heading --color=never '^publish[[:space:]]*=[[:space:]]*false$' "$LEGACY_CARGO"; then
  echo "[ERROR] icydb-model-legacy must remain unpublished while it exists." >&2
  status=1
fi

if ! rg -q --no-heading --color=never \
  'publication is blocked while the temporary icydb-model-legacy package exists' \
  scripts/ci/publish-workspace.sh
then
  echo "[ERROR] workspace publication must fail explicitly while icydb-model-legacy exists." >&2
  status=1
fi

legacy_core_edge="$(
  rg -n --no-heading --color=never 'icydb-model-legacy' "$CORE_CARGO" || true
)"
if [[ -n "$legacy_core_edge" ]]; then
  fail_with_matches \
    "icydb-core must not depend on the temporary model-authoring package." \
    "$legacy_core_edge"
fi

for required_application_owner in \
  'database_incarnation_id()' \
  'current_accepted_schema_root' \
  'stores.sort_by(|left, right| left.path.cmp(right.path))'
do
  if ! rg -q --fixed-strings "$required_application_owner" "$CORE_APPLICATION"; then
    echo "[ERROR] schema application targets must remain incarnation-, root-, and canonical-topology-owned: $required_application_owner" >&2
    status=1
  fi
done

application_model_leaks="$(
  rg -n --no-heading --color=never \
    '\b(EntityModel|CompiledSchemaProposal|EntityRuntimeHooks|E::MODEL)\b' \
    "$CORE_APPLICATION" || true
)"
if [[ -n "$application_model_leaks" ]]; then
  fail_with_matches \
    "schema application target issuance must not depend on generated model authority." \
    "$application_model_leaks"
fi

primitive_residue="$(
  rg -n --no-heading --color=never 'icydb-primitives|icydb_primitives' \
    Cargo.toml Cargo.lock crates scripts docs/contracts docs/FOUNDATIONS.md \
    --glob '!scripts/ci/check-schema-model-boundary-invariants.sh' || true
)"
if [[ -n "$primitive_residue" ]]; then
  fail_with_matches \
    "the retired standalone primitive owner must not survive after folding into icydb-schema." \
    "$primitive_residue"
fi

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
