#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch="$(mktemp -d)"
trap 'find "$scratch" -depth -delete' EXIT
cd "$ROOT"

# Use maintained sources in an isolated fixture: never mutate the worktree.
files=(
  scripts/ci/check-read-admission-invariants.sh
  scripts/ci/invariant-common.sh
  docs/contracts/READ_ADMISSION.md
  docs/guides/read-intent.md
  crates/icydb-core/src/db/query/admission/policy.rs
  crates/icydb-core/src/db/query/admission.rs
  crates/icydb-diagnostic-code/src/lib.rs
  crates/icydb/src/db/query/typed.rs
  crates/icydb/src/db/session/prepared_query.rs
  crates/icydb/src/db/session/sql.rs
  crates/icydb-model/src/build/actor/db/sql.rs
  crates/icydb-model/src/build/actor/endpoint.rs
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

echo "[OK] Read-admission invariant fixtures verified."
