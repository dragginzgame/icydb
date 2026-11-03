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
printf 'IcyDB runner completed status=0\n' >> "$attempt/runner.log"
set -- "$attempt"/logs/validation.*
[[ $# == 1 && -d "$1" ]] || exit 1
perl scripts/release/validation-receipt.pl "$attempt/plan.json" "$1" "$attempt/receipt.json"
