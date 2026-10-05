#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/icydb-workstation.XXXXXX")"
trap 'rm -rf "$FIXTURE"' EXIT
mkdir -p "$FIXTURE/scripts/dev" "$FIXTURE/scripts/ci" "$FIXTURE/bin" "$FIXTURE/outside"
mkdir -p "$FIXTURE/ci"
cp "$ROOT/ci/tool-versions.env" "$FIXTURE/ci/"
# shellcheck source=/dev/null
source "$ROOT/ci/tool-versions.env"
cp "$ROOT/scripts/dev/workstation-setup.sh" "$FIXTURE/scripts/dev/"
for script in install-gh.sh install-wasm-optimizer.sh verify-wasm-optimizer.sh \
  wasm-optimizer-pin.sh verify-file-checksum.sh; do
  cp "$ROOT/scripts/ci/$script" "$FIXTURE/scripts/ci/"
done
cp "$ROOT/scripts/ci/wasm-optimizer-checksums.tsv" "$FIXTURE/scripts/ci/"

export TEST_FIXTURE="$FIXTURE" TEST_HOST=Linux TEST_ARCH=x86_64
export TEST_TRACE="$FIXTURE/trace" TEST_EXECUTION="$FIXTURE/executed"
export TEST_TOOL_VERSION='wasm-opt version 132 (version_132)'
export PATH="$FIXTURE/bin:$PATH"

trace() { printf '%s\n' "$*" >> "$TEST_TRACE"; }
uname() {
  case "$1" in
    -s) printf '%s\n' "$TEST_HOST" ;;
    -m) printf '%s\n' "$TEST_ARCH" ;;
    *) return 2 ;;
  esac
}
rustup() { trace "rustup $* in $PWD"; }
cargo() { trace "cargo $* in $PWD"; }
npm() { trace "npm $*"; }
make() { trace "make $*"; }
gh() { trace "gh $*"; }
icp() { trace "icp $*"; }
function ic-wasm() { trace "ic-wasm $*"; }
brew() { trace "brew $*"; }
function xcode-select() { trace "xcode-select $*"; }
id() { printf '0\n'; }
export -f trace uname rustup cargo npm make gh icp ic-wasm brew xcode-select id

cat > "$FIXTURE/bin/apt-get" <<'TOOL'
#!/usr/bin/env bash
trace "apt-get $*"
TOOL
chmod +x "$FIXTURE/bin/apt-get"

cat > "$FIXTURE/bin/actionlint" <<'TOOL'
#!/usr/bin/env bash
echo 1.7.12
TOOL
chmod +x "$FIXTURE/bin/actionlint"
cat > "$FIXTURE/scripts/ci/install-icydb-actionlint.sh" <<'INSTALL'
#!/usr/bin/env bash
printf '%s\n' "$TEST_FIXTURE/bin/actionlint"
INSTALL
# Tool downloads and installations are stubbed for orchestration checks.
cp "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" "$FIXTURE/optimizer-installer"
cat > "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" <<'INSTALL'
#!/usr/bin/env bash
trace "optimizer $*"
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
    rg -F 'cargo install' "$TEST_TRACE" | rg -F cargo-watch | rg -F -- --locked >/dev/null
    rg -F "cargo install cargo-sort --version $SHARED_TOOLING_CARGO_SORT_VERSION --locked" "$TEST_TRACE" >/dev/null
    rg -F "cargo install cargo-sort-derives --version $ICYDB_CARGO_SORT_DERIVES_VERSION --locked" "$TEST_TRACE" >/dev/null
    rg -F "npm install -g --prefix $HOME/.local @icp-sdk/icp-cli @icp-sdk/ic-wasm" "$TEST_TRACE" >/dev/null
    rg -F "make --no-print-directory -C $FIXTURE install-hooks" "$TEST_TRACE" >/dev/null
    if [[ "$mode" == install ]]; then
      if [[ "$host" == Linux ]]; then
        rg -F 'apt-get install -y' "$TEST_TRACE" | rg -F cloc >/dev/null
      else
        rg -F 'brew install' "$TEST_TRACE" | rg -F cloc >/dev/null
      fi
    else
      rg -Fx 'optimizer --check-latest' "$TEST_TRACE" >/dev/null
    fi
  done
done

: > "$TEST_TRACE"
status=0
bash "$FIXTURE/scripts/dev/workstation-setup.sh" update extra > "$FIXTURE/invalid" 2>&1 || status=$?
[[ "$status" -eq 2 && ! -s "$TEST_TRACE" ]]

# Exercise missing-gh installation with only the declared macOS package manager.
mkdir -p "$FIXTURE/gh-bin"
ln -s /bin/bash "$FIXTURE/gh-bin/bash"
export TEST_HOST=Darwin
(
  unset -f gh
  export PATH="$TEST_FIXTURE/gh-bin"
  # Exported into the child installer.
  # shellcheck disable=SC2329
  brew() {
    trace "brew $*"
    printf '#!/bin/bash\nprintf "fixture-gh\\n"\n' > "$TEST_FIXTURE/gh-bin/gh"
    /bin/chmod +x "$TEST_FIXTURE/gh-bin/gh"
  }
  export -f brew
  /bin/bash "$TEST_FIXTURE/scripts/ci/install-gh.sh"
) > "$FIXTURE/gh-output"
rg -Fx 'brew install gh' "$TEST_TRACE" >/dev/null
rg -Fx fixture-gh "$FIXTURE/gh-output" >/dev/null

# Run the real Binaryen installer/verifier with disposable digest-pinned
# fixtures. Platform simulation proves selection, not native macOS execution.
mv "$FIXTURE/optimizer-installer" "$FIXTURE/scripts/ci/install-wasm-optimizer.sh"
mkdir -p "$FIXTURE/archive/binaryen-version_132/bin"
cat > "$FIXTURE/archive/binaryen-version_132/bin/wasm-opt" <<'TOOL'
#!/usr/bin/env bash
printf 'executed\n' >> "$TEST_EXECUTION"
printf '%s\n' "$TEST_TOOL_VERSION"
TOOL
chmod +x "$FIXTURE/archive/binaryen-version_132/bin/wasm-opt"
tar -czf "$FIXTURE/archive.tar.gz" -C "$FIXTURE/archive" binaryen-version_132
digest() {
  local output
  if command -v sha256sum >/dev/null 2>&1; then
    output="$(sha256sum "$1")"
  else
    output="$(shasum -a 256 "$1")"
  fi
  printf '%s\n' "${output%% *}"
}
archive_digest="$(digest "$FIXTURE/archive.tar.gz")"
binary_digest="$(digest "$FIXTURE/archive/binaryen-version_132/bin/wasm-opt")"
curl() {
  local output="" request=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --output) output="$2"; shift 2 ;;
      *) request="$1"; shift ;;
    esac
  done
  trace "$request"
  cp "$TEST_FIXTURE/archive.tar.gz" "$output"
}
export -f curl

for host_arch in Linux:x86_64 Darwin:x86_64 Darwin:arm64; do
  export TEST_HOST="${host_arch%:*}" TEST_ARCH="${host_arch#*:}"
  platform="$(bash -c 'ROOT="$TEST_FIXTURE"; source "$ROOT/scripts/ci/wasm-optimizer-pin.sh"; printf "%s" "$platform"')"
  asset="$(awk -v platform="$platform" '$1 == platform {print $2}' "$ROOT/scripts/ci/wasm-optimizer-checksums.tsv")"
  printf 'version\tversion_132\n%s\t%s\t%s\t%s\n' "$platform" "$asset" "$archive_digest" "$binary_digest" > "$FIXTURE/scripts/ci/wasm-optimizer-checksums.tsv"
  install_dir="$FIXTURE/install-$platform"
  WASM_OPT_INSTALL_DIR="$install_dir" bash "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" > "$FIXTURE/output"
  cmp "$install_dir/wasm-opt" "$FIXTURE/archive/binaryen-version_132/bin/wasm-opt"
  cp "$install_dir/wasm-opt" "$FIXTURE/installed.before"

  # A version mismatch cannot replace the previously verified installation.
  export TEST_TOOL_VERSION=wrong
  printf 'tampered\n' >> "$install_dir/wasm-opt"
  cp "$install_dir/wasm-opt" "$FIXTURE/tampered.before"
  status=0
  WASM_OPT_INSTALL_DIR="$install_dir" bash "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" > "$FIXTURE/rejected" 2>&1 || status=$?
  [[ "$status" -ne 0 ]]
  cmp "$install_dir/wasm-opt" "$FIXTURE/tampered.before"
  export TEST_TOOL_VERSION='wasm-opt version 132 (version_132)'
  cp "$FIXTURE/installed.before" "$install_dir/wasm-opt"

  # Reject changed download bytes before extracting or executing a candidate.
  executions_before="$(wc -l < "$TEST_EXECUTION")"
  printf 'version\tversion_132\n%s\t%s\t%064d\t%s\n' "$platform" "$asset" 0 "$binary_digest" > "$FIXTURE/scripts/ci/wasm-optimizer-checksums.tsv"
  status=0
  WASM_OPT_INSTALL_DIR="$FIXTURE/reject-$platform" bash "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" > "$FIXTURE/rejected" 2>&1 || status=$?
  [[ "$status" -ne 0 && ! -e "$FIXTURE/reject-$platform/wasm-opt" ]]
  [[ "$(wc -l < "$TEST_EXECUTION")" -eq "$executions_before" ]]

  # Even an intact archive must contain the admitted executable bytes.
  printf 'version\tversion_132\n%s\t%s\t%s\t%064d\n' "$platform" "$asset" "$archive_digest" 0 > "$FIXTURE/scripts/ci/wasm-optimizer-checksums.tsv"
  status=0
  WASM_OPT_INSTALL_DIR="$FIXTURE/reject-binary-$platform" bash "$FIXTURE/scripts/ci/install-wasm-optimizer.sh" > "$FIXTURE/rejected" 2>&1 || status=$?
  [[ "$status" -ne 0 && ! -e "$FIXTURE/reject-binary-$platform/wasm-opt" ]]
  [[ "$(wc -l < "$TEST_EXECUTION")" -eq "$executions_before" ]]
  cp "$ROOT/scripts/ci/wasm-optimizer-checksums.tsv" "$FIXTURE/scripts/ci/"
done

echo '[OK] workstation orchestration and Binaryen integrity boundaries verified (offline fixtures)'
