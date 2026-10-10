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

# Build one real structural checker for all copied-source cases. The fixture's
# Cargo adapter forwards source arguments only; it never resolves dependencies.
export CARGO_HOME="${CARGO_HOME:-$ROOT/.cache/cargo/icydb}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/icydb}"
cargo build --locked --offline --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p icydb-testing-integration --example check_runtime_panics
mkdir -p "$scratch/bin"
export TEST_RUNTIME_PANIC_CHECKER="$CARGO_TARGET_DIR/debug/examples/check_runtime_panics"
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
for pattern in 'value.unwrap();' 'value.expect ("reason");' 'panic!("reason");' \
  'panic! { "reason" };' 'assert!(false);' 'assert_eq!(1, 2);' 'assert_ne![1, 1];' \
  'unreachable!();' 'todo!();' 'unimplemented!();'; do
  printf 'fn production() { %s }\n' "$pattern" > "$executor/production.rs"
  check "panic-$cases" 1 bash "$panic_checker"
done
printf 'fn production() {}\n' > "$executor/production.rs"
for root in "${panic_roots[@]:1}"; do
  printf 'fn production() { value.unwrap(); }\n' > "$scratch/$root/production.rs"
  check "runtime-root-$cases" 1 bash "$panic_checker"
  printf 'fn production() {}\n' > "$scratch/$root/production.rs"
done
# The production inventory must not discard durable recovery files merely
# because Git ignores them or because they have no executor-owned neighbors.
printf 'fn recovery() { panic!("fixture"); }\n' > "$scratch/${panic_roots[1]}/recovery.rs"
printf '/recovery.rs\n' > "$scratch/${panic_roots[1]}/.gitignore"
check ignored-recovery-file 1 bash "$panic_checker"
rm "$scratch/${panic_roots[1]}/recovery.rs"
cat > "$executor/production.rs" <<'RUST'
const _: () =
    assert!(MAX_RECEIPT_BYTES <= MAX_PUBLIC_BYTES);
fn production() { debug_assert!(true); }
RUST
check compile-time-assertion 0 bash "$panic_checker"
printf 'fn production() { assert!(false); }\n' >> "$executor/production.rs"
check runtime-after-compile-time-assertion 1 bash "$panic_checker"
cat > "$executor/production.rs" <<'RUST'
const _: () =
    assert!(true); fn production() { panic!("fixture"); }
RUST
check runtime-beside-compile-time-assertion 1 bash "$panic_checker"
printf 'const fn production() { assert!(false); }\n' > "$executor/production.rs"
check runtime-callable-const-function 1 bash "$panic_checker"
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
cp "$executor/tests/case.rs" "$scratch/${panic_roots[1]}/convergence_candidate_tests.rs"
check test-exclusion 0 bash "$panic_checker"
# Structural item boundaries ignore lexical braces, including raw strings and
# nested comments, and still expose the production function after the test item.
for body in 'const EXAMPLE: &str = "{";' \
  'const EXAMPLE: &str = r###"{ \" }"###;' \
  '// {' '/* { /* } */ { */' \
  'fn test() { let _ = "}"; assert!(true); }'; do
  printf '#[cfg(test)]\nmod tests {\n%s\n}\n' "$body" > "$executor/production.rs"
  check "lexical-test-only-$cases" 0 bash "$panic_checker"
  printf 'fn production() { panic!("must be detected"); }\n' >> "$executor/production.rs"
  check "lexical-production-after-$cases" 1 bash "$panic_checker"
done
printf 'fn production() { let _ = r###"panic! { .unwrap()"###; /* panic!() */ }\n' > "$executor/production.rs"
check production-literal-and-comment 0 bash "$panic_checker"
printf 'fn production() { let _ = format!("{}", value.unwrap()); }\n' > "$executor/production.rs"
check production-inside-macro 1 bash "$panic_checker"
printf 'macro_rules! runtime { () => { panic!("fixture"); }; }\n' > "$executor/production.rs"
check production-macro-template 1 bash "$panic_checker"
printf '#[cfg(all(feature = "sql", test))]\nfn test() { panic!("fixture"); }\n' > "$executor/production.rs"
check reordered-test-cfg 0 bash "$panic_checker"
printf '#[cfg(not(not(test)))]\nfn test() { panic!("fixture"); }\n' > "$executor/production.rs"
check nested-test-cfg 0 bash "$panic_checker"
printf 'struct S; impl S { #[cfg(test)] fn test() { panic!("fixture"); } }\n' > "$executor/production.rs"
check test-only-associated-item 0 bash "$panic_checker"
printf 'fn production() {\n' > "$executor/production.rs"
check malformed-runtime-source 2 bash "$panic_checker"
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
printf '[OK] Invariant scanner fixtures passed (%s cases).\n' "$cases"
fixture_complete=true
