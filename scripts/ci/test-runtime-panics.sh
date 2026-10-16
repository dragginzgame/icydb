#!/usr/bin/env bash
# Shared companions: scripts/ci/check-runtime-panics.rs docs/rust-source-checks.md
set -euo pipefail

[[ "$#" -eq 1 && "$1" == /* && -x "$1" ]] || {
  echo "usage: test-runtime-panics.sh /absolute/path/to/compiled-scanner" >&2
  exit 2
}
checker="$1"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/runtime-panics.XXXXXX")"
complete=false
cleanup() {
  local status=$?
  [[ "$complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" -eq 0 ]]; then
    rm -rf "$scratch"
  else
    echo "Runtime scanner fixture retained: $scratch" >&2
  fi
  exit "$status"
}
trap cleanup EXIT
executor="$scratch/source"
mkdir -p "$executor"
cases=0
check() {
  local name="$1" expected="$2" actual=0
  shift 2
  "$@" > "$scratch/$name.stdout" 2> "$scratch/$name.stderr" || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    cat "$scratch/$name.stderr" >&2
    echo "Fixture $name: expected $expected, got $actual" >&2
    exit 1
  fi
  cases=$((cases + 1))
}
check no-input 2 "$checker"
check missing-input 2 "$checker" "$scratch/absent.rs"
for pattern in 'value.unwrap();' 'value.expect ("reason");' 'panic!("reason");' \
  'panic! { "reason" };' 'assert!(false);' 'assert_eq!(1, 2);' 'assert_ne![1, 1];' \
  'unreachable!();' 'todo!();' 'unimplemented!();'; do
  printf 'fn production() { %s }\n' "$pattern" > "$executor/production.rs"
  check "panic-$cases" 1 "$checker" "$executor/production.rs"
done
printf 'fn production() {}\n' > "$executor/production.rs"
cat > "$executor/production.rs" <<'RUST'
const _: () =
    assert!(MAX_RECEIPT_BYTES <= MAX_PUBLIC_BYTES);
fn production() { debug_assert!(true); }
RUST
check compile-time-assertion 0 "$checker" "$executor/production.rs"
printf 'fn production() { assert!(false); }\n' >> "$executor/production.rs"
check runtime-after-compile-time-assertion 1 "$checker" "$executor/production.rs"
cat > "$executor/production.rs" <<'RUST'
const _: () =
    assert!(true); fn production() { panic!("fixture"); }
RUST
check runtime-beside-compile-time-assertion 1 "$checker" "$executor/production.rs"
printf 'const fn production() { assert!(false); }\n' > "$executor/production.rs"
check runtime-callable-const-function 1 "$checker" "$executor/production.rs"
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
check test-exclusion 0 "$checker" "$executor/production.rs"
# Structural item boundaries ignore lexical braces, including raw strings and
# nested comments, and still expose the production function after the test item.
for body in 'const EXAMPLE: &str = "{";' \
  'const EXAMPLE: &str = r###"{ \" }"###;' \
  '// {' '/* { /* } */ { */' \
  'fn test() { let _ = "}"; assert!(true); }'; do
  printf '#[cfg(test)]\nmod tests {\n%s\n}\n' "$body" > "$executor/production.rs"
  check "lexical-test-only-$cases" 0 "$checker" "$executor/production.rs"
  printf 'fn production() { panic!("must be detected"); }\n' >> "$executor/production.rs"
  check "lexical-production-after-$cases" 1 "$checker" "$executor/production.rs"
done
printf 'fn production() { let _ = r###"panic! { .unwrap()"###; /* panic!() */ }\n' > "$executor/production.rs"
check production-literal-and-comment 0 "$checker" "$executor/production.rs"
printf 'fn production() { let _ = format!("{}", value.unwrap()); }\n' > "$executor/production.rs"
check production-inside-macro 1 "$checker" "$executor/production.rs"
printf 'macro_rules! runtime { () => { panic!("fixture"); }; }\n' > "$executor/production.rs"
check production-macro-template 1 "$checker" "$executor/production.rs"
printf '#[cfg(all(feature = "sql", test))]\nfn test() { panic!("fixture"); }\n' > "$executor/production.rs"
check reordered-test-cfg 0 "$checker" "$executor/production.rs"
printf '#[cfg(not(not(test)))]\nfn test() { panic!("fixture"); }\n' > "$executor/production.rs"
check nested-test-cfg 0 "$checker" "$executor/production.rs"
printf 'struct S; impl S { #[cfg(test)] fn test() { panic!("fixture"); } }\n' > "$executor/production.rs"
check test-only-associated-item 0 "$checker" "$executor/production.rs"
printf 'fn production() {\n' > "$executor/production.rs"
check malformed-runtime-source 2 "$checker" "$executor/production.rs"
printf '#[cfg(not(test))]\nfn production() { panic!("reason"); }\n' > "$executor/production.rs"
check production-cfg 1 "$checker" "$executor/production.rs"
printf '#[cfg(any(test, feature = "sql"))]\nfn production() { todo!(); }\n' > "$executor/production.rs"
check mixed-production-cfg 1 "$checker" "$executor/production.rs"

printf 'fn production() {}\n' > "$executor/production.rs"
check clean 0 "$checker" "$executor/production.rs"
printf '#![cfg(test)]\nfn test() { panic!("fixture"); }\n' > "$executor/production.rs"
check test-only-file 0 "$checker" "$executor/production.rs"
printf 'trait T { #[cfg(test)] fn test() { panic!("fixture"); } }\n' > "$executor/production.rs"
check test-only-trait-item 0 "$checker" "$executor/production.rs"
printf 'fn production() { value.unwrap(); }\n' > "$executor/production.rs"
check diagnostic 1 "$checker" "$executor/production.rs"
grep -F "$executor/production.rs:1:" "$scratch/diagnostic.stderr" > /dev/null
complete=true
printf '[OK] Shared runtime scanner fixtures passed (%s cases).\n' "$cases"
