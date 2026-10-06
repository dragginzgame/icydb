#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-invariant-scanners.XXXXXX")"
cleanup() {
  local status=$?
  if [[ "$status" -eq 0 ]]; then
    rm -rf "$scratch"
  else
    echo "Invariant scanner fixture retained at: $scratch" >&2
  fi
}
trap cleanup EXIT
mkdir -p "$scratch/scripts/ci"
for script in invariant-common.sh check-executor-no-production-panics.sh \
  check-sql-branch-ownership-invariants.sh; do
  cp "$ROOT/scripts/ci/$script" "$scratch/scripts/ci/"
done

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
check grouped-failure 2 bash -c 'set -euo pipefail; source "$1"; hits="$({ run_rg x "$2"; run_rg absent "$3"; } | strip_comment_only)"' \
  fixture "$common" "$scratch/missing.rs" "$scratch/source.rs"

executor="$scratch/crates/icydb-core/src/db/executor"
mkdir -p "$executor"
panic_checker="$scratch/scripts/ci/check-executor-no-production-panics.sh"
for pattern in 'value.unwrap();' 'value.expect ("reason");' 'panic!("reason");' \
  'panic! { "reason" };' 'assert!(false);' 'assert_eq!(1, 2);' 'assert_ne![1, 1];' \
  'unreachable!();' 'todo!();' 'unimplemented!();'; do
  printf 'fn production() { %s }\n' "$pattern" > "$executor/production.rs"
  check "panic-$cases" 1 bash "$panic_checker"
done
cat > "$executor/production.rs" <<'RUST'
fn production() { debug_assert!(true); debug_assert_eq!(1, 1); }
// panic!() describes the prohibition.
#[cfg(test)]
mod tests {
    fn test() { assert!(true); todo!(); }
}
#[cfg(test)]
fn standalone_test() { panic!("fixture"); }
#[cfg(all(test, feature = "sql"))]
fn feature_test() { unreachable!(); }
RUST
mkdir -p "$executor/tests"
printf 'fn test() { panic!("fixture"); }\n' > "$executor/tests/case.rs"
cp "$executor/tests/case.rs" "$executor/tests.rs"
cp "$executor/tests/case.rs" "$executor/case_tests.rs"
cp "$executor/tests/case.rs" "$executor/test_case.rs"
check test-exclusion 0 bash "$panic_checker"
printf '#[cfg(not(test))]\nfn production() { panic!("reason"); }\n' > "$executor/production.rs"
check production-cfg 1 bash "$panic_checker"
printf '#[cfg(any(test, feature = "sql"))]\nfn production() { todo!(); }\n' > "$executor/production.rs"
check mixed-production-cfg 1 bash "$panic_checker"
printf '/production.rs\n' > "$executor/.gitignore"
check ignored-production-file 1 bash "$panic_checker"
rm "$executor/production.rs"
check empty-inventory 1 bash "$panic_checker"
mv "$executor" "$scratch/retained-executor"
check missing-inventory 2 bash "$panic_checker"

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
printf '[OK] Invariant scanner fixtures passed (%s cases).\n' "$cases"
