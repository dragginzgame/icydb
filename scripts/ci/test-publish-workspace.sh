#!/usr/bin/env bash
set -euo pipefail

# Execute publication preflight, order and retries using command substitutes.
# No Git mutations, credentials, registry requests or real publication occur.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$root/.tools/host/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/publish-workspace.XXXXXX")"
trap '
    status=$?
    if [[ "$status" != 0 && -f "$fixture/output" ]]; then
        cat "$fixture/output" >&2 || true
    fi
    if [[ "$status" == 0 ]]; then rm -rf "$fixture"
    else echo "Publication fixture retained: $fixture" >&2; fi
    exit "$status"
' EXIT
mkdir -p "$fixture/bin" "$fixture/scripts/ci" "$fixture/state" "$fixture/receipts"
cp "$root/scripts/ci/publish-workspace.sh" \
    "$root/scripts/ci/verify-release-gate-receipt.sh" \
    "$root/scripts/ci/check-crates-io-version.sh" \
    "$root/scripts/ci/read-cargo-workspace-version.sh" "$fixture/scripts/ci/"
printf '[workspace.package]\nversion = "0.265.0"\n' > "$fixture/Cargo.toml"
crates=(icydb-diagnostic-code icydb-schema icydb-model-macros icydb-model icydb-core icydb icydb-cli)
for crate in "${crates[@]}"; do
    mkdir -p "$fixture/crates/$crate"
    printf '[package]\nname = "%s"\nversion.workspace = true\n' "$crate" \
        > "$fixture/crates/$crate/Cargo.toml"
done
export EVENTS="$fixture/events" FIXTURE_STATE="$fixture/state"
export RELEASE_RECEIPT_DIR="$fixture/receipts"
export HEAD_COMMIT=1111111111111111111111111111111111111111
real_bash="$(command -v bash)"
for command in git curl cargo sleep; do printf '#!%s\n' "$real_bash" > "$fixture/bin/$command"; done
cat >> "$fixture/bin/git" <<'GIT'
set -euo pipefail
if [[ "${1:-}" == -C ]]; then shift 2; fi
case "$*" in
    'diff --quiet --ignore-submodules HEAD --') [[ "${DIRTY:-0}" == 0 ]] ;;
    'ls-files --others --exclude-standard')
        [[ "${FAIL_INVENTORY:-0}" == 0 ]] || exit 9
        if [[ "${UNTRACKED:-0}" == 1 ]]; then echo untracked.rs; fi
        ;;
    'rev-parse --verify HEAD')
        [[ "${FAIL_HEAD:-0}" == 0 ]] || exit 9
        echo "$HEAD_COMMIT"
        ;;
    'cat-file -t refs/tags/v0.265.0') echo tag ;;
    'rev-parse --verify refs/tags/v0.265.0^{commit}') echo "$HEAD_COMMIT" ;;
    *) echo "Unexpected Git read: $*" >&2; exit 99 ;;
esac
GIT
cat >> "$fixture/bin/curl" <<'CURL'
set -euo pipefail
url="${*: -1}"
[[ "$url" == https://crates.io/api/v1/crates/*/0.265.0 ]]
crate="${url%/0.265.0}"
crate="${crate##*/}"
printf 'lookup %s 0.265.0\n' "$crate" >> "$EVENTS"
case "${REGISTRY_MODE:-present}" in
    present) printf 200 ;;
    absent) printf 404 ;;
    network-failure) exit 7 ;;
    server-failure) printf 503 ;;
    new|delayed|wait-failure)
        if [[ ! -f "$FIXTURE_STATE/$crate" ]]; then printf 404
        elif [[ "$REGISTRY_MODE" == wait-failure ]]; then printf 503
        elif [[ "$REGISTRY_MODE" == delayed && ! -f "$FIXTURE_STATE/$crate.visible" ]]; then
            touch "$FIXTURE_STATE/$crate.visible"
            printf 404
        else printf 200
        fi
        ;;
    *) exit 99 ;;
esac
CURL
cat >> "$fixture/bin/cargo" <<'CARGO'
set -euo pipefail
if [[ "$1" == locate-project ]]; then
    [[ "${FAIL_VERSION_READ:-0}" == 0 ]] || exit 7
    printf '%s\n' "$PWD/Cargo.toml"
    exit 0
fi
[[ "$1" == publish && "$2" == -p && "$4" == --locked && "$5" == --registry && "$6" == crates-io ]]
printf 'cargo %s\n' "$*" >> "$EVENTS"
[[ "${FAIL_PUBLISH:-0}" == 0 ]] || exit 7
case "$*" in *--dry-run*) ;; *) touch "$FIXTURE_STATE/$3" ;; esac
CARGO
cat >> "$fixture/bin/sleep" <<'SLEEP'
set -euo pipefail
printf 'sleep %s\n' "$*" >> "$EVENTS"
SLEEP
chmod +x "$fixture/bin/"*
export PATH="$fixture/bin:$PATH"

run_subject() {
    local expected="$1" status=0
    shift
    : > "$EVENTS"
    env PUBLISH_DRY_RUN=0 PUBLISH_VALIDATE_ONLY=0 PUBLISH_FROM= PUBLISH_VERIFY=auto \
        PUBLISH_POLL_SECS=10 PUBLISH_TIMEOUT_SECS=300 DIRTY=0 UNTRACKED=0 \
        FAIL_INVENTORY=0 FAIL_HEAD=0 FAIL_PUBLISH=0 REGISTRY_MODE=present \
        "$@" "$real_bash" "$fixture/scripts/ci/publish-workspace.sh" \
        > "$fixture/output" 2>&1 || status=$?
    if [[ "$expected" == fail ]]; then [[ "$status" != 0 ]]; else [[ "$status" == "$expected" ]]; fi
}

# Invalid selections and unavailable source admission stop before registry access.
for input in PUBLISH_FROM=missing PUBLISH_DRY_RUN=invalid PUBLISH_VERIFY=invalid \
    PUBLISH_VALIDATE_ONLY=invalid PUBLISH_POLL_SECS=0 PUBLISH_TIMEOUT_SECS=invalid \
    DIRTY=1 UNTRACKED=1 FAIL_INVENTORY=1 FAIL_HEAD=1; do
    run_subject fail "$input"
    [[ ! -s "$EVENTS" ]]
done
# Version observation failures stop before registry or publication effects.
cp "$fixture/Cargo.toml" "$fixture/original.toml"
printf "[workspace.package]\nversion = '0.265.0' # valid TOML\n" > "$fixture/Cargo.toml"
run_subject 0 PUBLISH_VALIDATE_ONLY=1
[[ ! -s "$EVENTS" ]]
run_subject fail FAIL_VERSION_READ=1
[[ ! -s "$EVENTS" ]]
printf '[workspace.package]\nversion = "0.265.0"\nversion = "invalid"\n' > "$fixture/Cargo.toml"
run_subject fail
[[ ! -s "$EVENTS" ]]
cp "$fixture/original.toml" "$fixture/Cargo.toml"
run_subject 0 PUBLISH_VALIDATE_ONLY=1
[[ ! -s "$EVENTS" ]]

# Exact existing versions skip publication even when a newer release exists.
run_subject 0 REGISTRY_MODE=present
for crate in "${crates[@]}"; do printf 'lookup %s 0.265.0\n' "$crate"; done > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
for mode in network-failure server-failure; do
    run_subject 2 "REGISTRY_MODE=$mode"
    printf 'lookup %s 0.265.0\n' "${crates[0]}" > "$fixture/expected"
    cmp "$fixture/expected" "$EVENTS"
done

# Missing receipts retain verification; valid receipts reuse the tested commit.
for receipt in missing valid stale; do
    rm -f "$RELEASE_RECEIPT_DIR/v0.265.0.commit"
    extra=''
    case "$receipt" in
        valid) printf '%s\n' "$HEAD_COMMIT" > "$RELEASE_RECEIPT_DIR/v0.265.0.commit"; extra=' --no-verify' ;;
        stale) printf '%040d\n' 2 > "$RELEASE_RECEIPT_DIR/v0.265.0.commit" ;;
    esac
    run_subject 0 REGISTRY_MODE=absent PUBLISH_DRY_RUN=1
    for crate in "${crates[@]}"; do
        printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --dry-run%s\n' "$crate" "$crate" "$extra"
    done > "$fixture/expected"
    cmp "$fixture/expected" "$EVENTS"
done
printf '%s\n' "$HEAD_COMMIT" > "$RELEASE_RECEIPT_DIR/v0.265.0.commit"
run_subject 0 REGISTRY_MODE=absent PUBLISH_DRY_RUN=1 PUBLISH_VERIFY=always PUBLISH_FROM=icydb
for crate in icydb icydb-cli; do
    printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --dry-run\n' "$crate" "$crate"
done > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"

# Publication waits for each prerequisite, and a retry skips completed versions.
run_subject 0 REGISTRY_MODE=delayed
for crate in "${crates[@]}"; do
    printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --no-verify\n' "$crate" "$crate"
    printf 'lookup %s 0.265.0\nsleep 10\nlookup %s 0.265.0\n' "$crate" "$crate"
done > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
run_subject 0 REGISTRY_MODE=new
for crate in "${crates[@]}"; do printf 'lookup %s 0.265.0\n' "$crate"; done > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"

rm -f "$FIXTURE_STATE/"*
run_subject 2 REGISTRY_MODE=wait-failure
printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --no-verify\nlookup %s 0.265.0\n' \
    "${crates[0]}" "${crates[0]}" "${crates[0]}" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
run_subject 0 REGISTRY_MODE=new
printf 'lookup %s 0.265.0\n' "${crates[0]}" > "$fixture/expected"
for crate in "${crates[@]:1}"; do
    printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --no-verify\nlookup %s 0.265.0\n' \
        "$crate" "$crate" "$crate" >> "$fixture/expected"
done
cmp "$fixture/expected" "$EVENTS"
rm -f "$FIXTURE_STATE/"*
run_subject 7 REGISTRY_MODE=new FAIL_PUBLISH=1
printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --no-verify\n' \
    "${crates[0]}" "${crates[0]}" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"

# Qualify the maintained Make entry point as well as direct script invocation.
: > "$EVENTS"
cd "$fixture"
env PUBLISH_DRY_RUN=1 PUBLISH_VALIDATE_ONLY=0 PUBLISH_FROM= PUBLISH_VERIFY=auto \
    PUBLISH_POLL_SECS=10 PUBLISH_TIMEOUT_SECS=300 REGISTRY_MODE=absent \
    make --no-print-directory -f "$root/Makefile" publish > "$fixture/output" 2>&1
for crate in "${crates[@]}"; do
    printf 'lookup %s 0.265.0\ncargo publish -p %s --locked --registry crates-io --dry-run --no-verify\n' \
        "$crate" "$crate"
done > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"
echo 'publication preflight, receipts, exact-version retries and order passed (command substitutes)'
