#!/usr/bin/env bash
set -euo pipefail

# Qualify IcyDB orchestration. Shared Tooling owns installer byte/version fixtures.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/icydb-workstation.XXXXXX")"
FIXTURE="$(cd "$FIXTURE" && pwd -P)"
trap 'status=$?; if [[ "$status" == 0 ]]; then rm -rf "$FIXTURE";
  else echo "Workstation fixture retained: $FIXTURE" >&2; fi; exit "$status"' EXIT
mkdir -p "$FIXTURE/scripts/dev" "$FIXTURE/scripts/ci" "$FIXTURE/bin" "$FIXTURE/outside" "$FIXTURE/ci" "$FIXTURE/make"
cp "$ROOT/ci/"{tool-versions.env,icydb-tools.env,ic-tools.tsv} "$FIXTURE/ci/"
cp "$ROOT/scripts/dev/workstation-setup.sh" "$FIXTURE/scripts/dev/"
cp "$ROOT/scripts/ci/install-gh.sh" "$FIXTURE/scripts/ci/"
# shellcheck source=/dev/null
source "$ROOT/ci/tool-versions.env"
# shellcheck source=/dev/null
source "$ROOT/ci/icydb-tools.env"
export TEST_FIXTURE="$FIXTURE" TEST_HOST=Linux TEST_ARCH=x86_64
export TEST_TRACE="$FIXTURE/trace"
export PATH="$FIXTURE/bin:$PATH"

trace() { printf '%s\n' "$*" >> "$TEST_TRACE"; }
# Invoked by the exported child setup fixture, then unset for native admission.
# shellcheck disable=SC2317,SC2329
uname() {
  case "$1" in
    -s) printf '%s\n' "$TEST_HOST" ;;
    -m) printf '%s\n' "$TEST_ARCH" ;;
    *) return 2 ;;
  esac
}
rustup() { trace "rustup $* in $PWD"; }
cargo() { trace "cargo $* in $PWD"; }
# Invoked by the child workstation script through the exported function.
# shellcheck disable=SC2317,SC2329
make() {
  trace "make $*"
  case "$*" in
    *tools-check) [[ "${TEST_CHECK_FAIL:-0}" == 0 ]] ;;
  esac
}
gh() { trace "gh $*"; }
brew() { trace "brew $*"; }
function xcode-select() { trace "xcode-select $*"; }
id() { printf '0\n'; }
export -f trace uname rustup cargo make gh brew xcode-select id
cat > "$FIXTURE/bin/apt-get" <<'TOOL'
#!/usr/bin/env bash
trace "apt-get $*"
TOOL
cat > "$FIXTURE/bin/actionlint" <<'TOOL'
#!/usr/bin/env bash
echo 1.7.12
TOOL
chmod +x "$FIXTURE/bin/"*
cat > "$FIXTURE/scripts/ci/install-icydb-actionlint.sh" <<'INSTALL'
#!/usr/bin/env bash
printf '%s\n' "$TEST_FIXTURE/bin/actionlint"
INSTALL
printf 'maintainer-selected dependency graph\n' > "$FIXTURE/Cargo.lock"
cp "$FIXTURE/Cargo.lock" "$FIXTURE/lock.before"
for host in Linux Darwin; do
  export TEST_HOST="$host"
  for mode in install update; do
    : > "$TEST_TRACE"
    (cd "$FIXTURE/outside"; bash "$FIXTURE/scripts/dev/workstation-setup.sh" "$mode") > "$FIXTURE/output"
    cmp "$FIXTURE/Cargo.lock" "$FIXTURE/lock.before"
    rg -F "rustup toolchain install --target wasm32-unknown-unknown in $FIXTURE" "$TEST_TRACE" >/dev/null
    for selection in "twiggy:$ICYDB_TWIGGY_VERSION" \
      "cargo-edit:$ICYDB_CARGO_EDIT_VERSION" \
      "cargo-watch:$ICYDB_CARGO_WATCH_VERSION"; do
      rg -F "${selection%:*} --version ${selection##*:} --locked" "$TEST_TRACE" >/dev/null
    done
    printf '%s\n' "make --no-print-directory -C $FIXTURE install-tools" \
      "make --no-print-directory -C $FIXTURE tools-check" \
      "make --no-print-directory -C $FIXTURE install-hooks" > "$FIXTURE/expected"
    rg '^make ' "$TEST_TRACE" > "$FIXTURE/actual"
    cmp "$FIXTURE/expected" "$FIXTURE/actual"
  done
done
# An offline tool refusal stops setup before hook activation.
: > "$TEST_TRACE"
status=0
TEST_CHECK_FAIL=1 bash "$FIXTURE/scripts/dev/workstation-setup.sh" update > "$FIXTURE/rejected" 2>&1 || status=$?
[[ "$status" != 0 ]]
if rg -F 'install-hooks' "$TEST_TRACE" >/dev/null; then exit 1; fi
: > "$TEST_TRACE"
status=0
bash "$FIXTURE/scripts/dev/workstation-setup.sh" update extra > "$FIXTURE/invalid" 2>&1 || status=$?
[[ "$status" == 2 && ! -s "$TEST_TRACE" ]]

# Exercise actual Make dispatch with local script stubs, without installations.
unset -f make
cp "$ROOT/Makefile" "$FIXTURE/Makefile"
cp "$ROOT/make/tools.mk" "$FIXTURE/make/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$FIXTURE/scripts/ci/"
for tool in rust host ic; do
  cat > "$FIXTURE/scripts/dev/install-$tool-tools.sh" <<'INSTALL'
#!/usr/bin/env bash
printf '%s %s\n' "${0##*/}" "$*" >> "$TEST_TRACE"
[[ "${TEST_INSTALL_FAIL:-0}" == 0 ]]
INSTALL
done
for script in check-pocketic-alignment.sh verify-wasm-optimizer.sh; do
  cat > "$FIXTURE/scripts/ci/$script" <<'POLICY'
#!/usr/bin/env bash
printf '%s\n' "${0##*/}" >> "$TEST_POLICY_TRACE"
[[ "${TEST_POLICY_FAIL:-0}" == 0 ]]
POLICY
done
export TEST_POLICY_TRACE="$FIXTURE/policy-trace"
: > "$TEST_TRACE"
command make --no-print-directory -C "$FIXTURE" > "$FIXTURE/default-goal"
[[ ! -s "$TEST_TRACE" ]]
command make --no-print-directory -C "$FIXTURE" install-tools > "$FIXTURE/dispatch"
printf '%s\n' "install-rust-tools.sh --consumer $FIXTURE --versions $FIXTURE/ci/tool-versions.env" \
  "install-host-tools.sh --consumer $FIXTURE --versions $FIXTURE/ci/tool-versions.env --with-ripgrep --with-cloc" \
  "install-ic-tools.sh --consumer $FIXTURE --pins $FIXTURE/ci/ic-tools.tsv" > "$FIXTURE/expected"
cmp "$FIXTURE/expected" "$TEST_TRACE"
: > "$TEST_TRACE"
command make --no-print-directory -C "$FIXTURE" tools-check > "$FIXTURE/offline"
printf '%s\n' "install-rust-tools.sh --consumer $FIXTURE --versions $FIXTURE/ci/tool-versions.env --check" \
  "install-host-tools.sh --consumer $FIXTURE --versions $FIXTURE/ci/tool-versions.env --with-ripgrep --with-cloc --check" \
  "install-ic-tools.sh --consumer $FIXTURE --pins $FIXTURE/ci/ic-tools.tsv --check" > "$FIXTURE/expected"
cmp "$FIXTURE/expected" "$TEST_TRACE"
printf '%s\n' check-pocketic-alignment.sh verify-wasm-optimizer.sh > "$FIXTURE/policy-expected"
cmp "$FIXTURE/policy-expected" "$TEST_POLICY_TRACE"
: > "$TEST_TRACE"
status=0
env TEST_POLICY_FAIL=1 make --no-print-directory -C "$FIXTURE" ic-tools-check \
  > "$FIXTURE/policy-refused" 2>&1 || status=$?
[[ "$status" != 0 ]]
: > "$TEST_TRACE"
status=0
# Use an external environment command so Bash 3.2 captures the failed child,
# rather than exiting at an assignment preceding the command builtin.
env TEST_INSTALL_FAIL=1 make --no-print-directory -C "$FIXTURE" install-tools > "$FIXTURE/failed-dispatch" 2>&1 || status=$?
[[ "$status" != 0 && "$(wc -l < "$TEST_TRACE")" -eq 1 ]]

# Provisioning cannot qualify a server that differs from the locked client.
cp "$ROOT/scripts/ci/check-pocketic-alignment.sh" "$FIXTURE/scripts/ci/"
printf '[[package]]\nname = "pocket-ic"\nversion = "16.0.0"\n' > "$FIXTURE/Cargo.lock"
bash "$FIXTURE/scripts/ci/check-pocketic-alignment.sh" > "$FIXTURE/aligned"
awk -F '\t' 'BEGIN { OFS="\t" } $1=="pocket-ic" && $3=="darwin-arm64" { $2="15.0.0" } { print }' \
  "$FIXTURE/ci/ic-tools.tsv" > "$FIXTURE/mismatched.tsv"
mv "$FIXTURE/mismatched.tsv" "$FIXTURE/ci/ic-tools.tsv"
if bash "$FIXTURE/scripts/ci/check-pocketic-alignment.sh" \
  > "$FIXTURE/mismatch" 2>&1; then exit 1; fi
cp "$ROOT/ci/ic-tools.tsv" "$FIXTURE/ci/ic-tools.tsv"
printf '[[package]]\nname = "other-client"\nversion = "16.0.0"\n' > "$FIXTURE/Cargo.lock"
if bash "$FIXTURE/scripts/ci/check-pocketic-alignment.sh" > "$FIXTURE/missing-client" 2>&1; then exit 1; fi

# IcyDB's raw optimizer admission is independent of shared archive provisioning.
# Check consumer ordering with a harmless executable; no real optimizer runs.
cp "$ROOT/scripts/ci/"{verify-wasm-optimizer.sh,verify-file-checksum.sh} "$FIXTURE/scripts/ci/"
unset -f uname
mkdir -p "$FIXTURE/.tools/ic/bin"
cat > "$FIXTURE/.tools/ic/bin/wasm-opt" <<'OPTIMIZER'
#!/usr/bin/env bash
printf 'executed\n' >> "$TEST_TRACE"
printf '%s\n' 'wasm-opt version 132 (version_132)'
OPTIMIZER
chmod +x "$FIXTURE/.tools/ic/bin/wasm-opt"
digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$FIXTURE/.tools/ic/bin/wasm-opt")"
for platform in linux_x86_64 darwin_x86_64 darwin_arm64; do
  printf '%s\t%s\n' "$platform" "$digest"
done > "$FIXTURE/scripts/ci/wasm-optimizer-checksums.tsv"
: > "$TEST_TRACE"
bash "$FIXTURE/scripts/ci/verify-wasm-optimizer.sh" > "$FIXTURE/optimizer-admitted"
[[ "$(wc -l < "$TEST_TRACE")" -eq 1 ]]
printf '\n# changed bytes\n' >> "$FIXTURE/.tools/ic/bin/wasm-opt"
: > "$TEST_TRACE"
if bash "$FIXTURE/scripts/ci/verify-wasm-optimizer.sh" > "$FIXTURE/optimizer-tampered" 2>&1; then exit 1; fi
[[ ! -s "$TEST_TRACE" ]]
perl -pi -e 's/version 132/version 133/' "$FIXTURE/.tools/ic/bin/wasm-opt"
digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$FIXTURE/.tools/ic/bin/wasm-opt")"
for platform in linux_x86_64 darwin_x86_64 darwin_arm64; do
  printf '%s\t%s\n' "$platform" "$digest"
done > "$FIXTURE/scripts/ci/wasm-optimizer-checksums.tsv"
: > "$TEST_TRACE"
if bash "$FIXTURE/scripts/ci/verify-wasm-optimizer.sh" > "$FIXTURE/optimizer-version-refused" 2>&1; then exit 1; fi
[[ "$(wc -l < "$TEST_TRACE")" -eq 1 ]]
awk -F '\t' 'BEGIN { OFS="\t" } $1=="wasm-opt" { $2="133" } { print }' \
  "$FIXTURE/ci/ic-tools.tsv" > "$FIXTURE/changed-optimizer.tsv"
mv "$FIXTURE/changed-optimizer.tsv" "$FIXTURE/ci/ic-tools.tsv"
: > "$TEST_TRACE"
if bash "$FIXTURE/scripts/ci/verify-wasm-optimizer.sh" > "$FIXTURE/optimizer-selection-refused" 2>&1; then exit 1; fi
[[ ! -s "$TEST_TRACE" ]]
echo '[OK] workstation and Make local-tool orchestration verified (offline substitutes)'
