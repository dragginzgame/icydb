#!/usr/bin/env bash
set -euo pipefail

# Exercise actual cleanup handlers before their fixture bodies can dispatch tools
# or release effects. A zero exit status alone cannot prove Bash 3.2 completion.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-fixture-completion.XXXXXX")"
fixture_complete=false
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$ROOT/scripts/ci/fixture-lifecycle.sh"
# shellcheck source=scripts/ci/fixture-retention-probes.sh
source "$ROOT/scripts/ci/fixture-retention-probes.sh"
trap 'fixture_finish "$?" "$fixture" "Fixture completion checks retained"' EXIT
# Darwin's implicit mktemp template can choose its per-user temporary directory
# instead of TMPDIR. Exercise that behavior on every host: a fixture must select
# its requested root explicitly to keep retained evidence inside the collector.
mkdir -p "$fixture/bin" "$fixture/requested" "$fixture/platform-default"
ICYDB_FIXTURE_REAL_MKTEMP="$(command -v mktemp)"
ICYDB_FIXTURE_NATIVE_TMPDIR="$fixture/platform-default"
export ICYDB_FIXTURE_REAL_MKTEMP ICYDB_FIXTURE_NATIVE_TMPDIR
cat > "$fixture/bin/mktemp" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# == 1 && "$1" == -d ]]; then
    exec "$ICYDB_FIXTURE_REAL_MKTEMP" -d "$ICYDB_FIXTURE_NATIVE_TMPDIR/tmp.XXXXXXXXXX"
fi
exec "$ICYDB_FIXTURE_REAL_MKTEMP" "$@"
STUB
chmod +x "$fixture/bin/mktemp"
export PATH="$fixture/bin:$PATH"
FIXTURE_PROBE_TEMPORARY_ROOT="$fixture/requested"
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
  # The shared probe requires explicit caller-owned variable names.
  FIXTURE_PROBE_ROOT_VARIABLE=ROOT
  FIXTURE_PROBE_DIRECTORY_VARIABLE=fixture
  case "$source" in
    scripts/release/test-standard-release.sh|scripts/release/test-receipt-callbacks.sh|scripts/release/test-lock-selection.sh|scripts/release/test-finalize-notes.sh|scripts/ci/test-publish-workspace.sh)
      FIXTURE_PROBE_ROOT_VARIABLE=root ;;
    scripts/ci/test-release-candidate-receipt.sh)
      FIXTURE_PROBE_ROOT_VARIABLE=ROOT_DIR ;;
  esac
  case "$source" in
    scripts/ci/test-workstation-setup.sh|scripts/ci/test-shared-tooling-adapters.sh)
      FIXTURE_PROBE_DIRECTORY_VARIABLE=FIXTURE ;;
    scripts/ci/test-invariant-scanners.sh|scripts/ci/test-read-admission-invariants.sh)
      FIXTURE_PROBE_DIRECTORY_VARIABLE=scratch ;;
    scripts/ci/test-wasm-audit-report.sh|scripts/ci/test-release-candidate-receipt.sh)
      FIXTURE_PROBE_DIRECTORY_VARIABLE=TEST_ROOT ;;
  esac
  for failure in nounset command nonzero premature completed failed-completion diagnostic; do
    if ! fixture_probe_exit "$ROOT/$source" "$fixture" "$ROOT" "${source##*/}" "$failure"; then
      printf 'Fixture completion failed: %s (%s)\n' "$source" "$failure" >&2
      fixture_replay_logs "$fixture/${source##*/}-$failure.log"
      exit 1
    fi
    cases=$((cases + 1))
  done
done
printf '[OK] Tooling fixture completion and retention passed: %s cases\n' "$cases"
fixture_complete=true
