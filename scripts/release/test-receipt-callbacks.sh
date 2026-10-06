#!/usr/bin/env bash
set -euo pipefail

# Exercise the real Make callbacks and receipt scripts without Git mutations.
# Candidate admission and Git reads are substitutes; receipts are real files.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/release-receipt-callbacks.XXXXXX")"
trap '
    status=$?
    if [[ "$status" != 0 && -f "$fixture/output" ]]; then
        cat "$fixture/output" >&2 || true
    fi
    rm -rf "$fixture"
    exit "$status"
' EXIT
mkdir -p "$fixture/bin" "$fixture/scripts/ci"
cp "$root/scripts/ci/record-release-gate-receipt.sh" \
    "$root/scripts/ci/verify-release-gate-receipt.sh" "$fixture/scripts/ci/"
if [[ -f "$root/tool-versions.env" ]]; then cp "$root/tool-versions.env" "$fixture/"; fi
printf '[workspace.package]\nversion = "0.1.1"\n' > "$fixture/Cargo.toml"
REAL_BASH="$(command -v bash)"
export REAL_BASH
real_make="$(command -v make)"
export HEAD_COMMIT=1111111111111111111111111111111111111111
export OTHER_COMMIT=2222222222222222222222222222222222222222
export EVENTS="$fixture/events"
export RELEASE_RECEIPT_DIR="$fixture/receipts"
printf '#!%s\n' "$REAL_BASH" > "$fixture/bin/bash"
cat >> "$fixture/bin/bash" <<'BASH'
set -euo pipefail
printf '%s\n' "$*" >> "$EVENTS"
if [[ "$*" == 'scripts/ci/release-candidate-receipt.sh verify-commit' ]]; then
    [[ "${FAIL_CANDIDATE:-0}" == 0 ]]
else
    exec "$REAL_BASH" "$@"
fi
BASH
printf '#!%s\n' "$REAL_BASH" > "$fixture/bin/git"
cat >> "$fixture/bin/git" <<'GIT'
set -euo pipefail
if [[ "${1:-}" == -C ]]; then shift 2; fi
case "$*" in
    'rev-parse --verify HEAD')
        [[ "${FAIL_HEAD:-0}" == 0 ]] || exit 9
        printf '%s\n' "$HEAD_COMMIT"
        ;;
    'cat-file -t refs/tags/v0.1.1') printf '%s\n' "${TAG_TYPE:-tag}" ;;
    'rev-parse --verify refs/tags/v0.1.1^{commit}')
        printf '%s\n' "${TAG_COMMIT:-$HEAD_COMMIT}"
        ;;
    *) echo "Unexpected Git command: $*" >&2; exit 99 ;;
esac
GIT
chmod +x "$fixture/bin/bash" "$fixture/bin/git"
export PATH="$fixture/bin:$PATH"
cd "$fixture"

run_callback() {
    local expected="$1" target="$2" status=0
    : > "$EVENTS"
    "$real_make" --no-print-directory -f "$root/Makefile" "$target" \
        > "$fixture/output" 2>&1 || status=$?
    if [[ "$expected" == pass ]]; then
        [[ "$status" == 0 ]]
    else
        [[ "$status" != 0 ]]
    fi
}

# The recorder and push checker must agree on the annotated tag's exact commit.
run_callback pass release-tagged-check
[[ "$(cat "$RELEASE_RECEIPT_DIR/v0.1.1.commit")" == "$HEAD_COMMIT" ]]
run_callback pass release-push-check
printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-commit' \
    "scripts/ci/verify-release-gate-receipt.sh $HEAD_COMMIT" > "$fixture/expected"
cmp "$fixture/expected" "$EVENTS"

# Missing/stale evidence and conflicting tags must never authorize a push.
rm "$RELEASE_RECEIPT_DIR/v0.1.1.commit"
run_callback fail release-push-check
printf '%s\n' "$OTHER_COMMIT" > "$RELEASE_RECEIPT_DIR/v0.1.1.commit"
run_callback fail release-push-check
printf '%s\n' "$HEAD_COMMIT" > "$RELEASE_RECEIPT_DIR/v0.1.1.commit"
TAG_TYPE=commit run_callback fail release-push-check
TAG_COMMIT="$OTHER_COMMIT" run_callback fail release-push-check
TAG_TYPE=commit run_callback fail release-tagged-check
TAG_COMMIT="$OTHER_COMMIT" run_callback fail release-tagged-check

# Failed source admission or HEAD discovery must stop before receipt verification.
for failure in FAIL_CANDIDATE FAIL_HEAD; do
    export "$failure=1"
    run_callback fail release-push-check
    unset "$failure"
    printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-commit' > "$fixture/expected"
    cmp "$fixture/expected" "$EVENTS"
done
echo 'release receipt Make callbacks passed (real receipt scripts; no Git effects)'
