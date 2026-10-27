#!/usr/bin/env bash
# Shared companions: scripts/ci/check-runtime-panics.rs scripts/ci/fixture-lifecycle.sh docs/rust-source-checks.md
set -euo pipefail

[[ "$#" -eq 1 && "$1" == /* && -x "$1" ]] || {
  echo "usage: test-runtime-panics.sh /absolute/path/to/compiled-scanner" >&2
  exit 2
}
checker="$1"
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "${BASH_SOURCE[0]%/*}/fixture-lifecycle.sh"
scratch="$(fixture_create "${TMPDIR:-/tmp}/runtime-panics.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$scratch" "Runtime scanner fixture retained"' EXIT
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
grep -F 'runtime panic checker requires a nonempty source inventory' "$scratch/no-input.stderr" > /dev/null
check help 0 "$checker" --help "$scratch/absent.rs"
grep -F 'Usage:' "$scratch/help.stdout" > /dev/null
[[ ! -s "$scratch/help.stderr" ]]
check unknown-option 2 "$checker" --unknown-scanner-option
grep -F 'unexpected argument' "$scratch/unknown-option.stderr" > /dev/null
check missing-input 2 "$checker" "$scratch/absent.rs"
# Clap must pass raw path bytes to the filesystem. Some filesystems (including
# macOS runners') reject these names: qualify the source-IO diagnostic there,
# rather than failing while constructing a filename the host cannot represent.
non_utf8_source="$executor/non-utf8-"$'\377'".rs"
if (printf 'fn valid() {}\n' > "$non_utf8_source") 2> "$scratch/non-utf8-create.stderr"; then
  check non-utf8-source 0 "$checker" "$non_utf8_source"
else
  check non-utf8-source 2 "$checker" "$non_utf8_source"
  grep -F 'cannot parse runtime source' "$scratch/non-utf8-source.stderr" > /dev/null
fi
printf 'fn valid() {}\n' > "$executor/-fixture.rs"
# shellcheck disable=SC2016 # Positional arguments expand in the child shell.
check hyphen-source 0 bash -c 'cd "$1"; exec "$2" -- -fixture.rs' -- "$executor" "$checker"
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
for pattern in 'outer!(value.unwrap::<u32>());' \
  'outer!(value.r#expect::<Result<Vec<u32>, Error>>("reason"));' \
  'outer!(value.unwrap::<fn() -> u32>());'; do
  printf 'fn production() { %s }\n' "$pattern" > "$executor/production.rs"
  check "macro-generic-method-$cases" 1 "$checker" "$executor/production.rs"
done
printf 'fn production() { outer!(value.unwrap::<u32>, value.expect < 3); }\n' > "$executor/production.rs"
check macro-method-reference 0 "$checker" "$executor/production.rs"
for pattern in 'value.r#unwrap();' 'value.r#expect("reason");' 'r#panic!("reason");' \
  'std::r#assert!(false);' 'format!("{}", value.r#unwrap());' \
  'outer!(r#panic!("reason"));'; do
  printf 'fn production() { %s }\n' "$pattern" > "$executor/production.rs"
  check "raw-identifier-$cases" 1 "$checker" "$executor/production.rs"
done
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
fixture_complete=true
printf '[OK] Shared runtime scanner fixtures passed (%s cases).\n' "$cases"
