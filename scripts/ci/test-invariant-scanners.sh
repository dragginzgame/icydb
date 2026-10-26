#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-invariant-scanners.XXXXXX")"
fixture_complete=false
cleanup() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" -eq 0 ]]; then
    rm -rf "$scratch"
  else
    echo "Invariant scanner fixture retained at: $scratch" >&2
  fi
  exit "$status"
}
trap cleanup EXIT
mkdir -p "$scratch/scripts/ci"
for script in invariant-common.sh check-executor-no-production-panics.sh \
  check-sql-branch-ownership-invariants.sh; do
  cp "$ROOT/scripts/ci/$script" "$scratch/scripts/ci/"
done

# The copied gate independently verifies the shared scanner source. Copy its
# authoritative manifest closure instead of maintaining another payload roster.
cp "$ROOT/.shared-tooling.snapshot" "$scratch/.shared-tooling.snapshot"
shared_records="$(bash "$ROOT/scripts/ci/verify-shared-tooling-snapshot.sh" \
  --read-manifest "$ROOT/.shared-tooling.snapshot")"
while IFS=$'\t' read -r kind _digest _mode shared_path; do
  [[ "$kind" == file ]] || continue
  mkdir -p "$(dirname "$scratch/$shared_path")"
  cp "$ROOT/$shared_path" "$scratch/$shared_path"
done <<< "$shared_records"

# Build one real structural checker for all copied-source cases. The fixture's
# Cargo adapter forwards source arguments only; it never resolves dependencies.
export CARGO_HOME="${CARGO_HOME:-$ROOT/.cache/cargo/icydb}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/icydb}"
cargo build --locked --offline --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p icydb-testing-source-checks --bin check_runtime_panics
mkdir -p "$scratch/bin"
export TEST_RUNTIME_PANIC_CHECKER="$CARGO_TARGET_DIR/debug/check_runtime_panics"
cat > "$scratch/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
while [[ "$#" -gt 0 && "$1" != -- ]]; do shift; done
[[ "$#" -gt 0 ]] || exit 2
shift
exec "$TEST_RUNTIME_PANIC_CHECKER" "$@"
CARGO
chmod +x "$scratch/bin/cargo"
export PATH="$scratch/bin:$PATH"

cases=0
check() {
  local name="$1" expected="$2" actual=0
  shift 2
  "$@" > "$scratch/$name.stdout" 2> "$scratch/$name.stderr" || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    echo "Fixture $name: expected status $expected, got $actual" >&2
    exit 1
  fi
  cases=$((cases + 1))
}

common="$scratch/scripts/ci/invariant-common.sh"
printf '%s\n' 'forbidden' '// forbidden' > "$scratch/source.rs"
search() {
  bash -c 'set -euo pipefail; source "$1"; hits="$(run_rg "$2" "$3" | strip_comment_only)"; printf "%s\n" "$hits"' \
    fixture "$common" "$@"
}
check matched 0 search forbidden "$scratch/source.rs"
printf '%s\n' "$scratch/source.rs:1:forbidden" > "$scratch/expected"
cmp "$scratch/expected" "$scratch/matched.stdout"
check no-match 0 search absent "$scratch/source.rs"
check missing-input 2 search forbidden "$scratch/missing.rs"
check invalid-regex 2 search '[' "$scratch/source.rs"
# A failing first search must not be hidden by a later successful search in a
# grouped pipeline, even when Bash disables errexit in command substitution.
# shellcheck disable=SC2016 # Positional parameters expand in the child shell.
check grouped-failure 2 bash -c 'set -euo pipefail; source "$1"; hits="$({ run_rg x "$2"; run_rg absent "$3"; } | strip_comment_only)"' \
  fixture "$common" "$scratch/missing.rs" "$scratch/source.rs"

panic_roots=(
  crates/icydb-core/src/db/executor
  crates/icydb-core/src/db/commit
  crates/icydb-core/src/db/journal
  crates/icydb-core/src/db/startup
)
for root in "${panic_roots[@]}"; do
  mkdir -p "$scratch/$root"
  printf 'fn production() {}\n' > "$scratch/$root/production.rs"
done
executor="$scratch/${panic_roots[0]}"
panic_checker="$scratch/scripts/ci/check-executor-no-production-panics.sh"
# Shared owns syntax regressions; retain only local root/discovery boundaries.
bash "$ROOT/scripts/ci/test-runtime-panics.sh" "$TEST_RUNTIME_PANIC_CHECKER"
printf 'fn production() {}\n' > "$executor/production.rs"
for root in "${panic_roots[@]:1}"; do
  printf 'fn production() { value.unwrap(); }\n' > "$scratch/$root/production.rs"
  check "runtime-root-$cases" 1 bash "$panic_checker"
  printf 'fn production() {}\n' > "$scratch/$root/production.rs"
done
printf 'fn production() {}\n' > "$executor/production.rs"
# Test-only filename exclusions belong to the local discovery inventory.
mkdir -p "$executor/tests"
printf 'fn test() { panic!("fixture"); }\n' > "$executor/tests/case.rs"
cp "$executor/tests/case.rs" "$executor/tests.rs"
cp "$executor/tests/case.rs" "$executor/case_tests.rs"
cp "$executor/tests/case.rs" "$executor/test_case.rs"
cp "$executor/tests/case.rs" "$scratch/${panic_roots[1]}/convergence_candidate_tests.rs"
check test-file-exclusion 0 bash "$panic_checker"
printf 'fn production() { panic!("fixture"); }\n' > "$scratch/${panic_roots[1]}/recovery.rs"
printf '/recovery.rs\n' > "$scratch/${panic_roots[1]}/.gitignore"
check ignored-recovery-file 1 bash "$panic_checker"
rm "$scratch/${panic_roots[1]}/recovery.rs"
printf 'fn production() { todo!(); }\n' > "$executor/production.rs"
printf '/production.rs\n' > "$executor/.gitignore"
check ignored-production-file 1 bash "$panic_checker"
rm "$executor/production.rs"
check empty-inventory 1 bash "$panic_checker"
mv "$executor" "$scratch/retained-executor"
check missing-inventory 2 bash "$panic_checker"
mv "$scratch/retained-executor" "$executor"
printf 'fn production() {}\n' > "$executor/production.rs"
for root in "${panic_roots[@]:1}"; do
  rm "$scratch/$root/production.rs"
  check "runtime-empty-$cases" 1 bash "$panic_checker"
  printf 'fn production() {}\n' > "$scratch/$root/production.rs"
  mv "$scratch/$root" "$scratch/absent-root"
  check "runtime-missing-$cases" 2 bash "$panic_checker"
  mv "$scratch/absent-root" "$scratch/$root"
done

# Qualify every SQL scan root, including terminal discovery. The product's
# required SELECT visibility owners are copied unchanged into the fixture.
roots=(
  crates/icydb-core/src/db/session/sql
  crates/icydb-core/src/db/executor
  crates/icydb-core/src/db/executor/terminal
  crates/icydb-core/src/db/sql/lowering
  crates/icydb-core/src/db/sql/parser
  crates/icydb-core/src/db/sql/lowering/select
)
for root in "${roots[@]}"; do
  mkdir -p "$scratch/$root"
  printf '// scan fixture\n' > "$scratch/$root/production.rs"
done
binding=crates/icydb-core/src/db/sql/lowering/select/binding.rs
cp "$ROOT/$binding" "$scratch/$binding"
sql_checker="$scratch/scripts/ci/check-sql-branch-ownership-invariants.sh"
check sql-baseline 0 bash "$sql_checker"
for root in "${roots[@]}"; do
  mv "$scratch/$root" "$scratch/absent-root"
  check "sql-missing-$cases" 2 bash "$sql_checker"
  mv "$scratch/absent-root" "$scratch/$root"
done
# Exercise policy classification independently from the runtime-source fixtures.
policy_root="$scratch/persisted-policy"
mkdir -p "$policy_root/scripts/ci" "$policy_root/crates" "$policy_root/testing"
cp "$ROOT/scripts/ci/check-persisted-format-invariants.sh" "$policy_root/scripts/ci/"
policy_checker="$policy_root/scripts/ci/check-persisted-format-invariants.sh"
for suffix in VERSION REVISION EPOCH GENERATION; do
  printf 'const FIXTURE_%s: u32 = 1;\n' "$suffix" > "$policy_root/crates/fixture.rs"
  check "policy-current-$suffix" 0 bash "$policy_checker"
  printf 'const FIXTURE_%s: u32 = 2;\n' "$suffix" > "$policy_root/crates/fixture.rs"
  check "policy-unclassified-$suffix" 1 bash "$policy_checker"
done
printf 'const CURRENT: Self = Self(2);\n' > "$policy_root/crates/fixture.rs"
check policy-current-wrapper 1 bash "$policy_checker"
rm "$policy_root/crates/fixture.rs"
policy_owner=crates/icydb-core/src/db/executor/mutation/job.rs
mkdir -p "$(dirname "$policy_root/$policy_owner")"
for component in ENTITY_REVISION MANAGED_WRITE_TIME EXECUTION_FAILURE; do
  printf 'const RESUMABLE_UPDATE_%s_POLICY_REVISION: u32 = 2;\n' "$component" > "$policy_root/$policy_owner"
  check "policy-approved-$component" 0 bash "$policy_checker"
  cp "$policy_root/$policy_owner" "$policy_root/crates/fixture.rs"
  check "policy-wrong-owner-$component" 1 bash "$policy_checker"
  rm "$policy_root/crates/fixture.rs"
  printf 'const RESUMABLE_UPDATE_%s_POLICY_REVISION: u32 = 3;\n' "$component" > "$policy_root/$policy_owner"
  check "policy-changed-value-$component" 1 bash "$policy_checker"
done
printf 'const OTHER_POLICY_REVISION: u32 = 2;\n' > "$policy_root/$policy_owner"
check policy-unapproved-name 1 bash "$policy_checker"
printf 'const RESUMABLE_UPDATE_ENTITY_REVISION_POLICY_REVISION: u32 = 2; const OTHER_REVISION: u32 = 2;\n' > "$policy_root/$policy_owner"
check policy-extra-declaration 1 bash "$policy_checker"
rm "$policy_root/$policy_owner"
mv "$policy_root/testing" "$policy_root/missing-testing"
check policy-missing-input 2 bash "$policy_checker"
printf '[OK] Invariant scanner fixtures passed (%s cases).\n' "$cases"
fixture_complete=true
