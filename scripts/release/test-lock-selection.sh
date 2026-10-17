#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$root/.tools/host/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-lock-selection.XXXXXX")"
fixture="$(cd -P "$fixture" && pwd)"
fixture_complete=false
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$root/scripts/ci/fixture-lifecycle.sh"
trap 'fixture_finish "$?" "$fixture" "Release lock fixture retained"' EXIT
mkdir -p "$fixture/base" "$fixture/candidate" "$fixture/bin"
for version in 0.1.0 0.1.1; do
    directory=base
    if [[ "$version" == 0.1.1 ]]; then directory=candidate; fi
    printf "[workspace]\n[workspace.package]\nversion = '%s' # release fixture\n[package]\nname = \"fixture\"\nversion.workspace = true\nedition = \"2024\"\n[lib]\npath = \"lib.rs\"\n" "$version" > "$fixture/$directory/Cargo.toml"
    printf 'pub fn fixture() {}\n' > "$fixture/$directory/lib.rs"
    printf 'version = 4\n\n[[package]]\nname = "fixture"\nversion = "%s"\n\n[[package]]\nname = "registry-package"\nversion = "0.1.0"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\nchecksum = "preserve0a1b0digest"\n' "$version" > "$fixture/$directory/Cargo.lock"
done
"${IC_BUILD_RELEASE_METADATA:?select the prepared editor}" --max-input-bytes 8388608 manifest \
    "$fixture/base/Cargo.toml" 0.1.0 0.1.1 > "$fixture/candidate/Cargo.toml"
real_bash="$(command -v bash)"
printf '#!%s\n' "$real_bash" > "$fixture/bin/git"
cat >> "$fixture/bin/git" <<'STUB'
set -euo pipefail
[[ "$1" != -C ]] || shift 2
case "$*" in
    'archive --format=tar 1111111111111111111111111111111111111111')
        [[ "${FAIL_ARCHIVE_READ:-0}" == 0 ]] || exit 9
        tar -cf - -C "$FIXTURE_BASE" .
        ;;
    'rev-parse --verify HEAD') printf '%s\n' 1111111111111111111111111111111111111111 ;;
    'show '*':Cargo.toml') cat "$FIXTURE_BASE/Cargo.toml"; [[ "${FAIL_CANDIDATE_READ:-0}" == 0 ]] || exit 19 ;;
    'show '*':Cargo.lock') cat "$FIXTURE_BASE/Cargo.lock" ;;
    'diff --cached --name-only -z HEAD --') ;;
    'diff --quiet --ignore-submodules HEAD --') exit 1 ;;
    'diff --name-only -z HEAD --') printf '%s\0' Cargo.toml Cargo.lock ;;
    'diff --binary HEAD --') printf '%s\n' immutable-fixture-transition ;;
    *) echo "unexpected fixture Git read: $*" >&2; exit 99 ;;
esac
STUB
chmod +x "$fixture/bin/git"
export FIXTURE_BASE="$fixture/base"
export PATH="$fixture/bin:$PATH"
export ICYDB_RELEASE_ROOT="$fixture/candidate" RELEASE_RECEIPT_DIR="$fixture/receipts"
subject="$root/scripts/ci/release-candidate-receipt.sh"
bash "$subject" record patch 1111111111111111111111111111111111111111 > "$fixture/output"
cp "$fixture/receipts/v0.1.1.candidate" "$fixture/accepted-receipt"
# A failed Git read or renderer must not authorize even complete-looking bytes.
cat > "$fixture/bin/failed-editor" <<'EDITOR'
#!/usr/bin/env bash
cat "$FIXTURE_BASE/../candidate/Cargo.toml"
exit 23
EDITOR
chmod +x "$fixture/bin/failed-editor"
for failure in source renderer; do
    renderer="$IC_BUILD_RELEASE_METADATA"
    failed_read=0
    if [[ "$failure" == source ]]; then failed_read=1; else renderer="$fixture/bin/failed-editor"; fi
    if FAIL_CANDIDATE_READ="$failed_read" IC_BUILD_RELEASE_METADATA="$renderer" \
        bash "$subject" record patch 1111111111111111111111111111111111111111 > "$fixture/output" 2>&1; then
        echo "failed $failure admitted a candidate" >&2; exit 1
    fi
    cmp "$fixture/accepted-receipt" "$fixture/receipts/v0.1.1.candidate"
done
mkdir "$fixture/failed-source"
if TMPDIR="$fixture/failed-source" FAIL_ARCHIVE_READ=1 \
  bash "$subject" record patch 1111111111111111111111111111111111111111 > "$fixture/output" 2>&1; then
    echo 'failed committed source observation admitted' >&2; exit 1
fi
cmp "$fixture/accepted-receipt" "$fixture/receipts/v0.1.1.candidate"
retained=0
for source in "$fixture/failed-source"/icydb-committed-version.*; do
    [[ -f "$source/source.tar" ]]
    retained=$((retained + 1))
done
[[ "$retained" == 1 ]]
cp "$fixture/candidate/Cargo.lock" "$fixture/accepted.lock"
for mutation in registry checksum; do
    if [[ "$mutation" == registry ]]; then
        sed '/name = "registry-package"/,$s/version = "0.1.0"/version = "0.1.1"/' "$fixture/accepted.lock" > "$fixture/candidate/Cargo.lock"
    else
        sed 's/preserve0a1b0digest/changed/' "$fixture/accepted.lock" > "$fixture/candidate/Cargo.lock"
    fi
    if bash "$subject" record patch 1111111111111111111111111111111111111111 > "$fixture/output" 2>&1; then
        echo "release receipt admitted changed $mutation selection" >&2; exit 1
    fi
    cp "$fixture/accepted.lock" "$fixture/candidate/Cargo.lock"
done
echo 'release lock selection passed (Cargo identities and Git read stubs)'
fixture_complete=true
