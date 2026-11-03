#!/usr/bin/env bash
# Pilot only. Does not alter release-verify, release metadata, refs or publication.
set -euo pipefail
[[ $# == 3 ]] || { echo 'usage: qualify-validation-plan.sh GROUP GROUP-TAG auto|workspace' >&2; exit 2; }
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
cd "$root"
export RELEASE_GROUP="$1" RELEASE_SOURCE
RELEASE_SOURCE="$(git rev-parse HEAD)"
[[ ! -L target && ! -L target/release-validation ]] || { echo 'redirected evidence directory' >&2; exit 1; }
mkdir -p target/release-validation
attempt="$(mktemp -d target/release-validation/qualification.XXXXXX)"
trap 'printf "Group validation evidence: %s\n" "$attempt"' EXIT
perl scripts/release/validation-plan.pl "$RELEASE_GROUP" "$RELEASE_SOURCE" "$2" "$attempt/plan.json" "$3"
VALIDATION_LOG_DIR="$attempt/logs" bash scripts/ci/run-icydb-validation-targets.sh --plan "$attempt/plan.json" > "$attempt/runner.log" 2>&1 || {
    cat "$attempt/runner.log" >&2
    exit 1
}
# Nested positional runners have their own logs. Select the unique invocation
# that retained this exact plan; unrelated/nested logs cannot supply its proof.
run=''
for candidate_run in "$attempt"/logs/validation.*; do
    retained_plan="$candidate_run/parsed-inputs.json.plan.json"
    [[ -d "$candidate_run" && ! -L "$candidate_run" && -f "$retained_plan" && ! -L "$retained_plan" ]] || continue
    if cmp -s "$attempt/plan.json" "$retained_plan"; then
        [[ -z "$run" ]] || { echo 'ambiguous plan invocation' >&2; exit 1; }
        run="$candidate_run"
    fi
done
[[ -n "$run" ]] || { echo 'missing plan invocation' >&2; exit 1; }
# Shared owns evidence admission and atomic byte custody; IcyDB owns authority.
# shellcheck source=scripts/ci/release-receipts.sh
source scripts/ci/release-receipts.sh
release_receipt_write "$attempt/evidence.json" perl scripts/ci/validation-evidence.pl verify \
    "$root" "$attempt/plan.json" "$RELEASE_SOURCE" "$RELEASE_GROUP" "$run" "$attempt/plan.json.context.json"
release_receipt_write "$attempt/receipt.json" perl scripts/release/validation-receipt.pl \
    "$attempt/plan.json" "$run" "$attempt/evidence.json"
printf '%s\n' "$attempt/receipt.json"
