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
    'show '*':Cargo.toml') cat "$FIXTURE_BASE/Cargo.toml" ;;
    'show '*':Cargo.lock') cat "$FIXTURE_BASE/Cargo.lock"; [[ "${FAIL_SOURCE_READ:-0}" == 0 ]] || exit 7 ;;
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
real_editor="${CARGO_RELEASE_METADATA_EDITOR:?prepare the released IC Build editor}"
export FIXTURE_REAL_EDITOR="$real_editor"
cat > "$fixture/bin/failing-editor" <<'EDITOR'
#!/usr/bin/env bash
set -euo pipefail
"$FIXTURE_REAL_EDITOR" "$@"
exit 7
EDITOR
chmod +x "$fixture/bin/failing-editor"
# Complete candidate bytes do not turn a failed producer into admission.
for producer in source editor; do
    selected_editor="$real_editor"
    fail_source=0
    if [[ "$producer" == editor ]]; then selected_editor="$fixture/bin/failing-editor";
    else fail_source=1; fi
    if CARGO_RELEASE_METADATA_EDITOR="$selected_editor" FAIL_SOURCE_READ="$fail_source" \
      bash "$subject" record patch 1111111111111111111111111111111111111111 > "$fixture/failed-$producer.log" 2>&1; then
        echo "release receipt admitted a failed $producer after complete output" >&2; exit 1
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
