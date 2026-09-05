#!/usr/bin/env bash
set -euo pipefail

# Bounded investigation evidence; invokes the maintained fixture suite in /tmp.
# Supply the checkout root explicitly. No real canister builds or report writes.
AUDITED_ROOT="${1:?usage: bash reproduce-baseline-selection.sh /path/to/icydb}"
source "$AUDITED_ROOT/scripts/ci/test-wasm-audit-report.sh"

# The sourced suite owns TEST_ROOT and its cleanup trap. Refresh its dirty actor.
write_fixture default_empty
subject="$FIXTURE/scripts/ci/wasm-audit-report.sh"
scope="$FIXTURE/docs/reports/recurring/2026/09/04/wasm-footprint"

bash "$subject" --skip-build --canister default_empty --date 2026-09-04
bash "$subject" --skip-build --canister default_empty --date 2026-09-04
bash "$subject" --skip-build --canister default_empty --date 2026-09-04 \
    --report-dir "$scope/03"

assert_baseline() {
    local report="$1" expected="$2"
    # Inspect the emitted metadata field, not an error-message string.
    local observed
    observed="$(sed -n 's/^- compared baseline report path: `\(.*\)`$/\1/p' "$report")"
    printf 'baseline: %s\n' "$observed"
    test "$observed" = "$expected"
}

# Positive control: automatic run 02 selects the day's run 01.
assert_baseline "$scope/02/report.md" \
    'docs/reports/recurring/2026/09/04/wasm-footprint/01/report.md'
# Reproduction: explicit canonical run 03 incorrectly selects run 02.
assert_baseline "$scope/03/report.md" \
    'docs/reports/recurring/2026/09/04/wasm-footprint/02/report.md'
printf 'REPRODUCED: explicit report directory bypasses daily run-01 baseline\n'

# Reproduction: a backdated run incorrectly chooses a later report as baseline.
bash "$subject" --skip-build --canister default_empty --date 2026-09-03
assert_baseline "$FIXTURE/docs/reports/recurring/2026/09/03/wasm-footprint/01/report.md" \
    'docs/reports/recurring/2026/09/04/wasm-footprint/03/report.md'
printf 'REPRODUCED: backdated run selects future baseline\n'
