#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch="$(mktemp -d)"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then find "$scratch" -depth -delete
  else echo "Read-admission fixture retained: $scratch" >&2; fi
  exit "$status"
}
trap finish EXIT
cd "$ROOT"

# Use maintained sources in an isolated fixture: never mutate the worktree.
files=(
  scripts/ci/check-read-admission-invariants.sh
  scripts/ci/invariant-common.sh
  docs/contracts/READ_ADMISSION.md
  crates/icydb-core/src/db/query/admission.rs
  crates/icydb-diagnostic-code/src/lib.rs
  crates/icydb/src/db/query/typed.rs
  crates/icydb/src/db/session/mod.rs
  crates/icydb/src/db/session/prepared_query.rs
  crates/icydb-model/src/build/actor/db/sql.rs
)
cp --parents "${files[@]}" "$scratch/"
checker="$scratch/scripts/ci/check-read-admission-invariants.sh"
diagnostics="crates/icydb-diagnostic-code/src/lib.rs"
contract="docs/contracts/READ_ADMISSION.md"

expect_failure() {
  if bash "$checker" >"$scratch/result.log" 2>&1; then
    echo "[ERROR] Read-admission fixture unexpectedly passed: $1" >&2
    exit 1
  fi
}

# Input diagnostics legitimately extend the plan-only rejection vocabulary.
bash "$checker"

# Narrative headings are not architecture or diagnostic identifiers.
sed -i '/^#/s/.*/# Reworded guidance/' "$scratch/$contract"
bash "$checker"
cp "$contract" "$scratch/$contract"

# A missing public counterpart must still reject.
sed -i '/^[[:space:]]*PublicQueryRequiresLimit,$/d' "$scratch/$diagnostics"
expect_failure "missing plan counterpart"
cp "$diagnostics" "$scratch/$diagnostics"

# Documentation is required even for errors raised before a plan exists.
sed -i '/QueryReadAdmissionCode::InputDepthExceeded/d' "$scratch/$contract"
expect_failure "missing input rejection documentation"
cp "$contract" "$scratch/$contract"

# Extraction failure must not silently turn the subset check into a no-op.
sed -i 's/pub enum QueryReadAdmissionCode {/pub enum FixtureDiagnostic {/' "$scratch/$diagnostics"
expect_failure "missing diagnostic inventory"
cp "$diagnostics" "$scratch/$diagnostics"

# Public exact terminals and private trusted SQL retain their dispatch boundaries.
for target in \
  'crates/icydb/src/db/query/typed.rs:execute_public_typed_exact_count' \
  'crates/icydb/src/db/session/mod.rs:execute_public_exact_count(' \
  'crates/icydb/src/db/session/mod.rs:execute_public_exact_count_for_typed_binding' \
  'crates/icydb/src/db/session/prepared_query.rs:execute_public_exact_key_batch_for_typed_binding' \
  'crates/icydb-model/src/build/actor/db/sql.rs:execute_trusted_sql_query_dispatch'; do
  file="${target%%:*}"
  symbol="${target#*:}"
  sed -i "s/$symbol/fixture_dispatch/g" "$scratch/$file"
  expect_failure "missing query dispatch handoff: $symbol"
  cp "$file" "$scratch/$file"
done
bash "$checker"

echo "[OK] Read-admission invariant fixtures verified."
fixture_complete=true
