#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
# shellcheck source=scripts/release/group.sh
source scripts/release/group.sh
# shellcheck source=scripts/ci/release-receipts.sh
source scripts/ci/release-receipts.sh
fallback() {
    printf 'Release validation: FULL (%s): %s\n' "$RELEASE_GROUP" "$1" >&2
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        printf '### Release validation: full (%s)\n\n%s\n\n' "$RELEASE_GROUP" "$1" >> "$GITHUB_STEP_SUMMARY"
    fi
    if [[ -n "${2:-}" ]]; then
        printf 'Selection diagnostics retained: %s\n' "$2" >&2
        cat "$2" >&2
    fi
    exec "${MAKE:-make}" --no-print-directory validate
}
if [[ "$RELEASE_GROUP" != cli ]]; then
    fallback 'Database subsystem selections are not qualified for release omissions.'
fi
source_commit="${RELEASE_SOURCE:?}"
receipts="$(release_receipt_directory)"
mkdir -p "$receipts" target/release-validation
pointer="$receipts/cli-$source_commit.gate"
if [[ -e "$pointer" || -L "$pointer" ]]; then
    release_receipt_archive "$receipts/previous-gate.XXXXXX" "$pointer"
    rm -- "$pointer"
fi
probe="$(mktemp -d target/release-validation/selection.XXXXXX)"
if ! perl scripts/release/validation-qualified.pl "$source_commit" > "$probe/qualification.json" 2> "$probe/qualification.log"; then
    fallback 'Native mapping, host/compiler or Cargo configuration is not qualified.' "$probe/qualification.log"
fi
version="${RELEASE_PREVIOUS:-$(bash scripts/release/read-committed-version.sh "$root" "$source_commit")}"
if ! perl scripts/release/validation-plan.pl cli "$source_commit" "cli-v$version" "$probe/plan.json" auto > "$probe/selection.log" 2>&1; then
    fallback 'Baseline/source admission requires the full gate.' "$probe/selection.log"
fi
if [[ "$(jq -r .scope "$probe/plan.json")" != group ]]; then
    jq -r '.changed[] | "Changed input: " + .' "$probe/plan.json.context.json" > "$probe/expansion.log"
    fallback 'Shared or uncertain changes exceed the CLI-private boundary.' "$probe/expansion.log"
fi
printf 'CLI release uses qualified selected validation.\n'
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '### Release validation: qualified CLI selection\n\n11 required stages; 4 qualified workspace exclusions.\n\n' >> "$GITHUB_STEP_SUMMARY"
fi
bash scripts/release/qualify-validation-plan.sh cli "cli-v$version" auto > "$probe/gate.log" 2>&1 || {
    cat "$probe/gate.log" >&2; exit 1;
}
receipt="$(sed -n '\|^target/release-validation/qualification\.[A-Za-z0-9]*/receipt.json$|p' "$probe/gate.log")"
[[ -n "$receipt" && "$receipt" != *$'\n'* ]]
gate="$(mktemp -d "$receipts/cli-$source_commit.XXXXXX")"
perl scripts/release/validation-gate.pl capture "$gate" "${receipt%/receipt.json}" > "$probe/admitted.json"
release_receipt_write "$pointer" printf '%s\n' "${gate##*/}"
printf 'Selected CLI validation retained: %s\n' "$gate"
