#!/usr/bin/env bash
set -euo pipefail

# Exercise actual cleanup handlers before their fixture bodies can dispatch tools
# or release effects. A zero exit status alone cannot prove Bash 3.2 completion.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-fixture-completion.XXXXXX")"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Fixture completion checks retained: $fixture" >&2; fi
  exit "$status"
}
trap finish EXIT
mkdir -p "$fixture/source/scripts/ci"
export COMPLETION_EXIT_PATH="$fixture/exit-path"
cases=0
for source in scripts/ci/test-workstation-setup.sh scripts/ci/test-testkit-tooling.sh \
    scripts/ci/test-ci-workflow-invariants.sh scripts/release/test-standard-release.sh \
    scripts/release/test-receipt-callbacks.sh scripts/ci/test-pre-commit.sh \
    scripts/ci/test-pocketic-server-wrapper.sh \
    scripts/ci/test-shared-tooling-adapters.sh scripts/ci/test-cargo-metadata-adoption.sh \
    scripts/release/test-lock-selection.sh scripts/release/test-pin-exceptions.sh \
    scripts/ci/test-publish-workspace.sh scripts/ci/test-invariant-scanners.sh \
    scripts/release/test-finalize-notes.sh scripts/ci/test-wasm-audit-report.sh \
    scripts/ci/test-read-admission-invariants.sh \
    scripts/ci/test-release-candidate-receipt.sh \
    scripts/ci/test-tooling-fixture-completion.sh; do
  for failure in nounset command nonzero premature completed failed-completion diagnostic; do
    # shellcheck disable=SC2016 # The disposable child expands this shell source.
    case "$failure" in
      nounset) injection='unset COMPLETION_UNBOUND; printf "%s\n" "$COMPLETION_UNBOUND"'; expected=1 ;;
      command) injection='false'; expected=1 ;;
      nonzero) injection='exit 23'; expected=23 ;;
      premature) injection='exit 0'; expected=1 ;;
      completed) injection='fixture_complete=true; exit 0'; expected=0 ;;
      failed-completion) injection='fixture_complete=true; exit 23'; expected=23 ;;
      diagnostic) injection='printf "fixture output\n" > "${fixture:-${TEST_ROOT:-${FIXTURE:-${scratch:-}}}}/output"; cat() { return 7; }; exit 23'; expected=23 ;;
    esac
    # ENVIRON preserves literal source bytes rather than decoding awk -v escapes.
    COMPLETION_INJECTION="$injection" awk '
      { print }
      /^trap (finish|cleanup) EXIT$/ {
        print "printf \"%s\\n\" \"${fixture:-${TEST_ROOT:-${FIXTURE:-${scratch:-}}}}\" > \"$COMPLETION_EXIT_PATH\""
        print ENVIRON["COMPLETION_INJECTION"]
        print "exit 99"
        injected=1
      }
      END { if (!injected) exit 1 }
    ' "$ROOT/$source" > "$fixture/source/scripts/ci/probe.sh"
    status=0
    TMPDIR="$fixture" "$BASH" "$fixture/source/scripts/ci/probe.sh" \
      > "$fixture/${source##*/}-$failure.log" 2>&1 || status=$?
    if [[ "$status" != "$expected" ]]; then
      printf 'Fixture %s/%s: expected status %s, observed %s\n' \
        "$source" "$failure" "$expected" "$status" >&2
      exit 1
    fi
    retained="$(cat "$COMPLETION_EXIT_PATH")"
    [[ -n "$retained" ]]
    if [[ "$expected" == 0 ]]; then [[ ! -e "$retained" ]]
    else [[ -d "$retained" ]]; fi
    cases=$((cases + 1))
  done
done
printf '[OK] Tooling fixture completion and retention passed: %s cases\n' "$cases"
fixture_complete=true
