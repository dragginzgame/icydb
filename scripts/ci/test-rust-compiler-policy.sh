#!/usr/bin/env bash
set -euo pipefail

# Exercise the actual consumer action's policy projection and compiler admission.
# Rustup installation belongs to its pinned action; this fixture has no downloads.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-compiler-policy.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else printf 'Compiler fixture retained: %s\n' "$fixture" >&2; fi
}
trap finish EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/bin"
mkdir -p "$fixture/vendor/toko-tooling/scripts/ci/"
cp "$ROOT/vendor/toko-tooling/scripts/ci/read-cargo-workspace-rust-version.sh" "$fixture/vendor/toko-tooling/scripts/ci/"
mkdir -p "$fixture/scripts/ci/"
cp "$ROOT/scripts/ci/check-rust-compiler.sh" "$fixture/scripts/ci/"
for tool in cargo rustc; do
  cat > "$fixture/bin/$tool" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == --version ]]
name="${0##*/}"
if [[ "$name" == cargo ]]; then version="$TEST_CARGO_VERSION"; else version="$TEST_RUSTC_VERSION"; fi
printf '%s %s (fixture)\n' "$name" "$version"
printf '%s\n' "$name" >> "$COMPILER_TRACE"
STUB
  chmod +x "$fixture/bin/$tool"
done
export PATH="$fixture/bin:$PATH"
export TEST_CARGO_VERSION=1.99.1 TEST_RUSTC_VERSION=1.99.1
export COMPILER_TRACE="$fixture/compiler-trace"
export GITHUB_OUTPUT="$fixture/output" GITHUB_ENV="$fixture/environment"
export GITHUB_WORKSPACE="$fixture"
"${YQ:-yq}" -p yaml -o json '.' "$ROOT/.github/actions/prepare-rust/action.yml" > "$fixture/action.json"
jq -r '.runs.steps[] | select(.id == "policy") | .run' "$fixture/action.json" > "$fixture/project.sh"
jq -r '.runs.steps[] | select(.name == "Admit actual Cargo and rustc identities") | .run' "$fixture/action.json" > "$fixture/admit.sh"
jq -e '.runs.steps | any(.[]; (.uses // "" | startswith("dtolnay/rust-toolchain@"))
  and .with.toolchain == "${{ steps.policy.outputs.version }}"
  and .with.components == "${{ inputs.components }}" and .with.targets == "${{ inputs.targets }}")' "$fixture/action.json" > /dev/null
cd "$fixture"
reset() {
  printf '[workspace.package]\nrust-version = "1.99.1"\n' > Cargo.toml
  printf '[toolchain]\nchannel = "1.99.1"\n' > rust-toolchain.toml
  : > "$COMPILER_TRACE"
}
count=0
check() {
  local expected="$1" name="$2" status=0
  bash -e -o pipefail "$fixture/admit.sh" > "$fixture/$name.log" 2>&1 || status=$?
  if [[ "$expected" == pass && "$status" != 0 || "$expected" == fail && "$status" == 0 ]]; then
    printf 'Compiler case %s expected %s, observed %s\n' "$name" "$expected" "$status" >&2
    cat "$fixture/$name.log" >&2
    exit 1
  fi
  count=$((count + 1))
}
reset
bash -e -o pipefail "$fixture/project.sh"
printf 'version=1.99.1\n' > expected-output
printf 'RUSTUP_TOOLCHAIN=1.99.1\nRUST_CURRENT=1.99.1\n' > expected-environment
cmp expected-output "$GITHUB_OUTPUT"
cmp expected-environment "$GITHUB_ENV"
check pass declared-compiler
printf 'rustc\ncargo\n' > expected-trace
cmp expected-trace "$COMPILER_TRACE"
reset
TEST_CARGO_VERSION=1.99.0 check fail wrong-cargo
reset
TEST_RUSTC_VERSION=1.99.0 check fail wrong-rustc
reset
printf '[toolchain]\nchannel = "stable"\n' > rust-toolchain.toml
check fail moving-toolchain
[[ ! -s "$COMPILER_TRACE" ]]
reset
printf '[workspace.package]\nrust-version = "1.96.0"\n' > Cargo.toml
check fail mismatched-floor
[[ ! -s "$COMPILER_TRACE" ]]
reset
printf '[workspace.package]\nrust-version = "1.99.2"\n' > Cargo.toml
printf '[toolchain]\nchannel = "1.99.2"\n' > rust-toolchain.toml
TEST_CARGO_VERSION=1.99.2 TEST_RUSTC_VERSION=1.99.2 check pass projected-policy
printf '[OK] Consumer compiler policy passed: %s cases and actual action projection\n' "$count"
