#!/usr/bin/env bash
# Exercise the real ordered Make dispatch with harmless fixture bodies.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# shellcheck source=vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh
source "$root/vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh"
fixture="$(fixture_create "${TMPDIR:-/tmp}/icydb-automation-dispatch.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$fixture" "Automation dispatch retained"' EXIT
mkdir -p "$fixture/vendor/toko-tooling/scripts/ci"
for name in run-validation-targets.sh check-make-execution.sh verify-file-checksum.sh \
  validation-input-checkpoint.pl release-validation-plan.pl ValidationPlan.pm; do
  cp "$root/vendor/toko-tooling/scripts/ci/$name" "$fixture/vendor/toko-tooling/scripts/ci/"
done
# Only replace leaf fixture bodies. Keep the actual roster and caller recipe.
awk '
  /^AUTOMATION_TARGETS :=/ { copying=1 }
  /^\.PHONY: check-qualification-tooling/ { copying=0 }
  copying && /^_automation-.*:$/ {
    print; print "\t@bash fixture.sh $@"; leaf=1; next
  }
  copying && leaf && /^\t/ { next }
  copying { print }
' "$root/Makefile" > "$fixture/Makefile"
cat >> "$fixture/Makefile" <<'MAKE'
VALIDATION_RUNNER := VALIDATION_REPOSITORY_ROOT="$(CURDIR)" bash vendor/toko-tooling/scripts/ci/run-validation-targets.sh --input watched
roster:
	@printf '%s\n' $(AUTOMATION_TARGETS)
MAKE
cat > "$fixture/fixture.sh" <<'SH'
set -eu
printf '%s\n' "$1" >> calls
if [ "${FAIL_TARGET:-}" = "$1" ]; then exit 23; fi
if [ "${DRIFT_TARGET:-}" = "$1" ]; then printf 'changed\n' >> watched; fi
SH
cd "$fixture"
make --no-print-directory roster > expected
# A dropped or repeated fixture must not silently shrink the timing roster.
[[ "$(sort -u expected | wc -l | tr -d ' ')" == 15 ]]
for mode in pass failure drift; do
  printf 'initial\n' > watched
  : > calls
  fail_target='' drift_target=''
  [[ "$mode" != failure ]] || fail_target=_automation-cargo-metadata
  [[ "$mode" != drift ]] || drift_target=_automation-cargo-metadata
  status=0
  FAIL_TARGET="$fail_target" DRIFT_TARGET="$drift_target" \
    VALIDATION_LOG_DIR="$fixture/$mode" make --no-print-directory check-automation \
    > "$fixture/$mode.log" 2>&1 || status=$?
  if [[ "$mode" == pass ]]; then
    [[ "$status" == 0 ]]
    cmp expected calls
  else
    [[ "$status" != 0 ]]
    head -n 3 expected > stopped
    cmp stopped calls
  fi
  # Each executed target owns one timed row; skipped fixtures have no fake pass.
  reports=("$fixture/$mode"/validation.*/timings.tsv)
  [[ ${#reports[@]} == 1 && -f "${reports[0]}" ]]
  awk -F '\t' 'NR>1 {print $1; if ($3 !~ /^[0-9]+$/) exit 1}' "${reports[0]}" > timed
  cmp calls timed
  if [[ "$mode" != pass ]]; then
    awk -F '\t' 'END {exit ($2 != "FAIL")}' "${reports[0]}"
  fi
done
fixture_complete=true
echo 'Automation dispatch: complete roster, timings, failure and input drift passed.'
