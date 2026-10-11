#!/usr/bin/env bash
set -euo pipefail

# These isolated Make calls select their own flags and recursive context.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES
# Select fixture thread defaults independently of CI's ambient libtest cap.
# Explicit inherited-cap forwarding is qualified below through actual Make.
unset RUST_TEST_THREADS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/icydb-shared-adapters.XXXXXX")"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then rm -rf "$FIXTURE"
  else echo "Shared adapter fixture retained: $FIXTURE" >&2; fi
  exit "$status"
}
trap finish EXIT
mkdir -p "$FIXTURE/scripts/ci" "$FIXTURE/bin" "$FIXTURE/install"
for script in check-make-execution.sh run-validation-targets.sh run-icydb-validation-targets.sh \
  install-actionlint.sh install-ci-tool.sh install-icydb-actionlint.sh verify-file-checksum.sh; do
  cp "$ROOT/scripts/ci/$script" "$FIXTURE/scripts/ci/"
done

cat > "$FIXTURE/Makefile" <<'MAKE'
.PHONY: pass fail-one fail-two nested
pass:
	@echo pass-payload
	@touch executed
fail-one:
	@echo first-complete-payload
	@echo 'error: first-diagnostic'
	@echo 'fatal: adapter-early-fatal'
	@i=0; while [ $$i -lt 200 ]; do echo ordinary-tail; i=$$((i + 1)); done
	@exit 7
fail-two:
	@echo second-complete-payload
	@echo 'error: second-diagnostic'
	@exit 9
nested:
	+@bash scripts/ci/run-icydb-validation-targets.sh --fail-fast fail-one
parallel:
	+@bash scripts/ci/run-icydb-validation-targets.sh selections
selections:
	@test "$(RELEASE_VERSION)" = 0.266.1
	@test "$(RELEASE_REMOTE)" = reviewed
	@test "$(RELEASE_BRANCH)" = main
	@test "$(SELECTION_WITH_SPACE)" = 'kept value'
depth:
	@printf '%s\n' "$$VALIDATION_RUNNER_DEPTH" > depth-dispatched
MAKE

# Shared runner and installer failure permutations are qualified upstream at the
# revision in .shared-tooling.snapshot. Keep these tests on the consumer boundary:
# repository/log selection, argument/status forwarding and pin/asset selection.
# Inherited metadata reaches the canonical runner through the real adapter;
# the adapter must still select its own repository rather than the caller's.
for depth in 0 4; do
  (
    cd "$FIXTURE/bin"
    VALIDATION_REPOSITORY_ROOT="$FIXTURE/bin" VALIDATION_RUNNER_DEPTH="$depth" \
      ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/depth-failures" \
      bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" depth
  ) > "$FIXTURE/depth-output" 2>&1
  [[ "$(cat "$FIXTURE/depth-dispatched")" == "$((depth + 1))" ]]
done
rm "$FIXTURE/depth-dispatched"
status=0
(
  cd "$FIXTURE/bin"
  VALIDATION_REPOSITORY_ROOT="$FIXTURE/bin" VALIDATION_RUNNER_DEPTH=ICYDB_UNBOUND_DEPTH \
    VALIDATION_LOG_DIR="$FIXTURE/depth-rejected-logs" \
    ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/depth-rejected-failures" \
    bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" depth
) > "$FIXTURE/depth-rejected-output" 2>&1 || status=$?
[[ "$status" == 2 ]]
[[ ! -e "$FIXTURE/depth-dispatched" && ! -e "$FIXTURE/depth-rejected-logs" && \
   ! -e "$FIXTURE/depth-rejected-failures" ]]

# Run from another directory so the adapter must select its own repository.
status=0
(
  cd "$FIXTURE/bin"
  ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/logs" \
    GITHUB_STEP_SUMMARY="$FIXTURE/adapter-summary.md" \
    bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" --fail-fast pass fail-one fail-two
) > "$FIXTURE/output" 2>&1 || status=$?
# Make reports a failed recipe as status 2; preserve that through both owners.
[[ "$status" -eq 2 ]]
[[ -f "$FIXTURE/executed" ]]
rg -F first-complete-payload "$FIXTURE/logs/latest-combined.log" > /dev/null
grep -Fx '[ERR:fail-one] fatal: adapter-early-fatal' "$FIXTURE/output" > /dev/null
grep -Fx '[ERR:fail-one] fatal: adapter-early-fatal' "$FIXTURE/logs/latest-errors.log" > /dev/null
grep -Fx '[ERR:fail-one] fatal: adapter-early-fatal' "$FIXTURE/adapter-summary.md" > /dev/null
if rg -F second-complete-payload "$FIXTURE/output" > /dev/null; then exit 1; fi

status=0
ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/nested-logs" \
  bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" nested \
  > "$FIXTURE/nested-output" 2>&1 || status=$?
[[ "$status" -eq 2 ]]
rg -F first-complete-payload "$FIXTURE/nested-logs/latest-combined.log" > /dev/null

# Legitimate selections and the live jobserver survive nested adapter dispatch.
ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/parallel-logs" \
  make --no-print-directory -C "$FIXTURE" -j2 parallel RELEASE_VERSION=0.266.1 \
    RELEASE_REMOTE=reviewed RELEASE_BRANCH=main 'SELECTION_WITH_SPACE=kept value' \
    > "$FIXTURE/parallel-output" 2>&1
if rg -i 'jobserver unavailable|jobserver.*invalid|forced in submake' "$FIXTURE/parallel-output" >/dev/null; then
  cat "$FIXTURE/parallel-output" >&2
  exit 1
fi

# Qualify actual Cargo entrypoints without compiling or running product
# gates. Cargo substitutes inspect live descriptors, cache roots and libtest
# concurrency; the real consumer Makefile and shared dispatch remain in use.
native="$FIXTURE/native"
mkdir -p "$native/make" "$native/scripts/ci" "$native/bin"
cp "$ROOT/Makefile" "$native/"
cp "$ROOT/make/"*.mk "$native/make/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$native/scripts/ci/"
for script in check-make-execution.sh run-validation-targets.sh run-icydb-validation-targets.sh \
  wasm-size-report.sh wasm-audit-report.sh wasm-report-common.sh verify-file-checksum.sh; do
  cp "$ROOT/scripts/ci/$script" "$native/scripts/ci/"
done
export CI_CARGO_ROOT="$native" CI_CARGO_TRACE="$native/cargo-trace"
export CI_CARGO_SERVER_TRACE="$native/server-trace"
export CI_CARGO_ARGUMENT_TRACE="$native/argument-trace"
cat > "$native/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
[[ "${MAKEFLAGS:-}" =~ --jobserver-(auth|fds)=([0-9]+),([0-9]+) ]]
reader="${BASH_REMATCH[2]}"; writer="${BASH_REMATCH[3]}"
: <&"$reader"
: >&"$writer"
[[ "$CARGO_HOME" == "${CI_CARGO_EXPECTED_HOME:-$CI_CARGO_ROOT/.cache/cargo/icydb}" ]]
[[ "$CARGO_TARGET_DIR" == "${CI_CARGO_EXPECTED_TARGET:-$CI_CARGO_ROOT/target/icydb}" ]]
printf '%s %s\n' "$1" "${RUST_TEST_THREADS:-default}" >> "$CI_CARGO_TRACE"
printf '%s\n' "$*" >> "$CI_CARGO_ARGUMENT_TRACE"
if [[ "${TMPDIR:-}" == "$CI_CARGO_ROOT/.cache" ]]; then
  [[ "${POCKET_IC_BIN:-}" == "$CI_CARGO_ROOT/testkit-selected-server" ]]
  printf '%s %s\n' "$1" "${RUST_TEST_THREADS:-default}" >> "$CI_CARGO_SERVER_TRACE"
fi
if [[ "${CI_CARGO_FAIL:-0}" == 1 &&
      ( -z "${CI_CARGO_FAIL_COMMAND:-}" || "$1" == "$CI_CARGO_FAIL_COMMAND" ) ]]; then
  echo native-cargo-retained-payload
  echo 'error: native-cargo-failure' >&2
  exit 23
fi
case "$1 ${2:-}" in
  'locate-project --workspace') printf '%s\n' "$CI_CARGO_ROOT/Cargo.toml" ;;
  'sort --version') printf 'cargo-sort 2.1.4\n' ;;
esac
if [[ -n "${ICYDB_SQL_TIER_C_ARTIFACT_DIR:-}" ]]; then
  [[ "$ICYDB_SQL_TIER_C_ARTIFACT_DIR" == "$CI_CARGO_ROOT/tier-c" ]]
  receipt="$ICYDB_SQL_TIER_C_ARTIFACT_DIR/tier-c-merged.json"
  if [[ -n "${ICYDB_SQL_TIER_C_SHARD_INDEX:-}" ]]; then
    [[ "$ICYDB_SQL_TIER_C_SHARD_INDEX" == 0 ]]
    receipt="$ICYDB_SQL_TIER_C_ARTIFACT_DIR/tier-c-shard-0.json"
  fi
  [[ ! -e "$receipt" ]]
  if [[ "${CI_CARGO_OMIT_RECEIPT:-0}" != 1 ]]; then
    printf 'fixture-current-receipt\n' > "$receipt"
  fi
fi
if [[ -n "${ICYDB_SQL_TIER_C_FAILURE_ARTIFACT:-}" ]]; then
  [[ "$ICYDB_SQL_TIER_C_FAILURE_ARTIFACT" == "$CI_CARGO_ROOT/failure.fixture.json" ]]
fi
CARGO
chmod +x "$native/bin/cargo"
for lane in ci-core ci-workspace ci-sql-tier-a; do
  PATH="$native/bin:$PATH" ICYDB_VALIDATION_FAILURE_LOG_DIR="$native/failure-logs" \
    make --no-print-directory -C "$native" -j2 "$lane" > "$native/$lane.log" 2>&1
done
printf '%s\n' 'check default' 'check default' 'check default' \
  'clippy default' 'clippy default' 'test 8' \
  'clippy default' 'clippy default' 'test 2' \
  'test default' 'test default' 'test 2' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"

: > "$CI_CARGO_TRACE"
PATH="$native/bin:$PATH" RUST_TEST_THREADS=3 \
  make --no-print-directory -C "$native" -j2 ci-core > "$native/inherited-threads.log" 2>&1
printf '%s\n' 'check 3' 'check 3' 'check 3' 'clippy 3' 'clippy 3' 'test 8' \
  > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"

# The explicit caller selection is only a path forwarded to substitute Cargo;
# no server starts and no application-owned server admission is introduced.
: > "$CI_CARGO_TRACE"
: > "$CI_CARGO_SERVER_TRACE"
for lane in test test-no-default-smoke test-durability test-integration-feedback test-sql-canister-matrix; do
  PATH="$native/bin:$PATH" ICYDB_VALIDATION_FAILURE_LOG_DIR="$native/local-failure-logs" \
    make --no-print-directory -C "$native" -j2 "$lane" \
    "POCKET_IC_BIN=$native/testkit-selected-server" TEST_TARGET=fixture-test TEST_NAME=fixture-case \
    > "$native/$lane.log" 2>&1
done
printf '%s\n' 'test default' 'test 8' 'test 2' 'test 2' 'test default' \
  'test default' 'test 8' 'test default' 'test default' 'test 2' \
  'test 2' 'test 2' 'test 2' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"
printf '%s\n' 'test 2' 'test 2' 'test 2' 'test 2' 'test 2' 'test 2' \
  > "$native/expected-server-trace"
cmp "$native/expected-server-trace" "$CI_CARGO_SERVER_TRACE"

# Failed Testkit admission and incomplete feedback selection stop before Cargo.
: > "$CI_CARGO_TRACE"
status=0
PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 _test-workspace \
  POCKET_IC_BIN= TESTKIT_SERVER_CHECK=false > "$native/admission-refusal.log" 2>&1 || status=$?
[[ "$status" == 2 && ! -s "$CI_CARGO_TRACE" ]]
status=0
PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 test-integration-feedback \
  TEST_TARGET=fixture-test TEST_NAME= > "$native/feedback-refusal.log" 2>&1 || status=$?
[[ "$status" == 2 && ! -s "$CI_CARGO_TRACE" ]]

# Direct builders retain their profile and canister selections. Development,
# documentation and feature recipes share the same transport, with static Perl
# effects substituted so this probe does not run broad product gates.
: > "$CI_CARGO_TRACE"
: > "$CI_CARGO_ARGUMENT_TRACE"
for lane in build-canister-local build-canister-production; do
  PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$lane" \
    CANISTER=default_empty > "$native/$lane.log" 2>&1
done
printf '%s\n' \
  'run --locked -p icydb-testing-integration --bin build_fixture_canister -- default_empty --build-profile local --profile debug --candid-export on' \
  'run --locked -p icydb-testing-integration --bin build_fixture_canister -- default_empty --build-profile production --profile wasm-release --candid-export on' \
  > "$native/expected-arguments"
cmp "$native/expected-arguments" "$CI_CARGO_ARGUMENT_TRACE"
mkdir "$native/doc-bin"
printf '#!/usr/bin/env bash\nexit 0\n' > "$native/doc-bin/perl"
chmod +x "$native/doc-bin/perl"
for lane in fetch build check clippy test-documentation check-feature-matrix clean test-watch; do
  PATH="$native/doc-bin:$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$lane" \
    > "$native/$lane.log" 2>&1
done
printf '%s\n' 'run default' 'run default' 'fetch default' 'build default' \
  'check default' 'clippy default' 'clippy default' 'clippy default' \
  'test default' 'test default' 'test default' 'test default' \
  'test default' 'test default' 'test default' 'test default' \
  'check default' 'check default' 'check default' 'check default' 'check default' \
  'clean default' 'watch default' \
  > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"
: > "$CI_CARGO_TRACE"
status=0
PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 build-canister-local \
  CANISTER= > "$native/build-refusal.log" 2>&1 || status=$?
[[ "$status" == 2 && ! -s "$CI_CARGO_TRACE" ]]

# Follow the real size/audit shell chain to its first Cargo child. A controlled
# builder failure must reach Make after descriptor admission and leave existing
# artifact bytes intact. No synthetic report claims product qualification.
for tool in ic-wasm wasm-opt candid-extractor twiggy; do
  printf '#!/usr/bin/env bash\nexit 0\n' > "$native/bin/$tool"
  chmod +x "$native/bin/$tool"
done
mkdir -p "$native/artifacts/wasm-size"
printf 'previous-artifact\n' > "$native/artifacts/wasm-size/retained.wasm"
for lane in wasm-size-report wasm-audit-report; do
  : > "$CI_CARGO_TRACE"
  status=0
  PATH="$native/bin:$PATH" CI_CARGO_FAIL=1 \
    make --no-print-directory -C "$native" -j2 "$lane" \
    'SIZE_REPORT_ARGS=--canister default_empty' \
    "AUDIT_REPORT_ARGS=--canister default_empty --report-dir $native/audit-failure" \
    > "$native/$lane.log" 2>&1 || status=$?
  [[ "$status" == 2 ]]
  printf '%s\n' 'run default' > "$native/expected-trace"
  cmp "$native/expected-trace" "$CI_CARGO_TRACE"
  rg -F native-cargo-retained-payload "$native/$lane.log" >/dev/null
  [[ "$(cat "$native/artifacts/wasm-size/retained.wasm")" == previous-artifact ]]
done

# Tier C clears stale receipts before Cargo and requires a newly emitted one;
# its replay keeps the exact caller-selected artifact and single-thread option.
: > "$CI_CARGO_TRACE"
: > "$CI_CARGO_ARGUMENT_TRACE"
mkdir "$native/tier-c"
for lane in test-sql-tier-c-shard test-sql-tier-c-merge test-sql-tier-c-replay; do
  PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$lane" \
    TIER_C_SHARD=0 "TIER_C_ARTIFACT_DIR=$native/tier-c" \
    "TIER_C_FAILURE_ARTIFACT=$native/failure.fixture.json" > "$native/$lane.log" 2>&1
done
printf '%s\n' 'test default' 'test default' 'test default' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"
for owner in shard merge replay; do
  case "$owner" in
    shard) selection=tier_c_native_shard_emits_exact_receipt ;;
    merge) selection=tier_c_native_receipts_merge_exactly_and_require_clean_evidence ;;
    replay) selection=tier_c_failure_artifact_replays_exact_minimized_failure ;;
  esac
  printf 'test --locked -p icydb-core --lib --features sql db::session::tests::tier_c_reference::%s -- --ignored --exact --nocapture --test-threads=1\n' "$selection"
done > "$native/expected-arguments"
cmp "$native/expected-arguments" "$CI_CARGO_ARGUMENT_TRACE"
for owner in shard merge; do
  receipt="$native/tier-c/tier-c-merged.json"
  if [[ "$owner" == shard ]]; then receipt="$native/tier-c/tier-c-shard-0.json"; fi
  printf 'stale-receipt\n' > "$receipt"
  status=0
  PATH="$native/bin:$PATH" CI_CARGO_OMIT_RECEIPT=1 \
    make --no-print-directory -C "$native" -j2 "test-sql-tier-c-$owner" \
    TIER_C_SHARD=0 "TIER_C_ARTIFACT_DIR=$native/tier-c" \
    > "$native/$owner-missing-receipt.log" 2>&1 || status=$?
  [[ "$status" == 2 && ! -e "$receipt" ]]
done
for lane in test-sql-tier-c-shard test-sql-tier-c-replay; do
  : > "$CI_CARGO_TRACE"
  status=0
  PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$lane" \
    TIER_C_SHARD= TIER_C_FAILURE_ARTIFACT= > "$native/$lane-refusal.log" 2>&1 || status=$?
  [[ "$status" == 2 && ! -s "$CI_CARGO_TRACE" ]]
done

# The managed server is substituted only at the existing wrapper boundary;
# actual outer Make, shared dispatch and inner Tier B recipes stay in use.
printf '#!/usr/bin/env bash\nset -euo pipefail\nexec "$@"\n' > "$native/bin/managed-run"
chmod +x "$native/bin/managed-run"
mkdir -p "$native/.cache"
: > "$CI_CARGO_TRACE"
: > "$CI_CARGO_SERVER_TRACE"
PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 ci-sql-tier-b \
  "POCKET_IC_RUNNER=$native/bin/managed-run" "POCKET_IC_BIN=$native/testkit-selected-server" \
  > "$native/ci-sql-tier-b.log" 2>&1
printf '%s\n' 'test 2' 'test 2' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"
cmp "$native/expected-trace" "$CI_CARGO_SERVER_TRACE"

# Metadata and release callbacks must retain the same transport as compilers.
# Keep real version/pin admission, formatting and preparation control flow;
# substitute only version mutation and candidate receipt effects.
mkdir -p "$native/scripts/release" "$native/ci" "$native/docs/changelog" "$native/.tools/host/bin"
cp "$ROOT/ci/tool-versions.env" "$native/ci/"
cp "$ROOT/.tools/host/bin/yq" "$native/.tools/host/bin/"
for script in read-cargo-workspace-version.sh check-dependency-pins.sh dependency-pins.jq \
  check-format-tools.sh run-formatting.sh finalize-release-changelog.awk; do
  cp "$ROOT/scripts/ci/$script" "$native/scripts/ci/"
done
cp "$ROOT/scripts/release/"{prepare.sh,files.sh,check-metadata.sh,finalize-notes.sh} "$native/scripts/release/"
printf '[workspace.package]\nversion = "0.1.1"\n' > "$native/Cargo.toml"
printf 'version = 4\n' > "$native/Cargo.lock"
printf '.cache/\n' > "$native/.gitignore"
printf '## [0.1.1] - 2026-10-10\n' > "$native/CHANGELOG.md"
cp "$native/CHANGELOG.md" "$native/docs/changelog/0.1.md"
printf '[]\n' > "$native/ci/dependency-pinning-exceptions.json"
printf '#!/usr/bin/env bash\nset -euo pipefail\ncargo set-version --help\n' > "$native/scripts/ci/bump-version.sh"
# The receipt substitute expands the selected working directory in its child.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\nset -euo pipefail\ncargo locate-project --workspace --message-format plain --manifest-path "$PWD/Cargo.toml"\n' \
  > "$native/scripts/ci/release-candidate-receipt.sh"
cp "$native/scripts/ci/release-candidate-receipt.sh" "$native/scripts/ci/record-release-gate-receipt.sh"
# Git inventory is real, with no commits; the receipt command above models only
# its Cargo transport, while the dedicated receipt fixture owns its semantics.
git -C "$native" init --quiet
git -C "$native" add Cargo.toml Cargo.lock
mkdir -p "$native/.cache/release-tmp"
: > "$CI_CARGO_TRACE"
for lane in version release-version check-versioning check-dependency-pins \
  release-prepared-check release-commit-check release-committed-check release-tagged-check \
  release-prepare-version; do
  if [[ "$lane" == release-prepare-version ]]; then
    printf '## [0.1.1]\n' > "$native/CHANGELOG.md"
  else
    printf '## [0.1.1] - 2026-10-10\n' > "$native/CHANGELOG.md"
  fi
  cp "$native/CHANGELOG.md" "$native/docs/changelog/0.1.md"
  PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$lane" \
    RELEASE_VERSION=0.1.1 RELEASE_PREVIOUS=0.1.0 RELEASE_KIND=patch RELEASE_DATE=2026-10-10 \
    RELEASE_SOURCE=1111111111111111111111111111111111111111 \
    > "$native/$lane.log" 2>&1
done
printf '%s\n' 'locate-project default' 'locate-project default' \
  'set-version default' 'locate-project default' 'locate-project default' \
  'locate-project default' 'metadata default' 'locate-project default' \
  'locate-project default' 'metadata default' 'locate-project default' \
  'locate-project default' 'locate-project default' 'locate-project default' \
  'set-version default' 'sort default' 'fmt default' 'sort-derives default' \
  'sort default' 'fmt default' 'locate-project default' 'metadata default' \
  'locate-project default' 'locate-project default' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"
for failure in early metadata; do
for lane in release-prepared-check release-commit-check release-prepare-version; do
  if [[ "$lane" == release-prepare-version ]]; then
    printf '## [0.1.1]\n' > "$native/CHANGELOG.md"
  else
    printf '## [0.1.1] - 2026-10-10\n' > "$native/CHANGELOG.md"
  fi
  cp "$native/CHANGELOG.md" "$native/docs/changelog/0.1.md"
  cp "$native/CHANGELOG.md" "$native/notes-before"
  : > "$CI_CARGO_TRACE"
  status=0
  fail_command=''
  if [[ "$failure" == metadata ]]; then fail_command=metadata; fi
  PATH="$native/bin:$PATH" CI_CARGO_FAIL=1 CI_CARGO_FAIL_COMMAND="$fail_command" \
    make --no-print-directory -C "$native" -j2 "$lane" \
    RELEASE_VERSION=0.1.1 RELEASE_PREVIOUS=0.1.0 RELEASE_KIND=patch RELEASE_DATE=2026-10-10 \
    > "$native/failure-$failure-$lane.log" 2>&1 || status=$?
  [[ "$status" == 2 ]]
  if [[ "$failure" == early ]]; then
    [[ "$(wc -l < "$CI_CARGO_TRACE")" == 1 ]]
  else
    [[ "$(tail -n 1 "$CI_CARGO_TRACE")" == 'metadata default' ]]
  fi
  rg -F native-cargo-failure "$native/failure-$failure-$lane.log" >/dev/null
  cmp "$native/notes-before" "$native/CHANGELOG.md"
  cmp "$native/docs/changelog/0.1.md" "$native/CHANGELOG.md"
done
done

# CLI installation must keep the user's selected Cargo paths while preserving
# the same parallel descriptors. The substitute never installs a binary.
: > "$CI_CARGO_TRACE"
PATH="$native/bin:$PATH" CARGO_HOME="$native/caller-home" \
  CARGO_TARGET_DIR="$native/caller-target" CI_CARGO_EXPECTED_HOME="$native/caller-home" \
  CI_CARGO_EXPECTED_TARGET="$native/caller-target" \
  make --no-print-directory -C "$native" -j2 install > "$native/install.log" 2>&1
printf '%s\n' 'install default' > "$native/expected-trace"
cmp "$native/expected-trace" "$CI_CARGO_TRACE"

for mode in -n -t -q -i; do
  for lane in ci-core test build-canister-local wasm-size-report wasm-audit-report \
    fetch build check clippy test-documentation check-feature-matrix clean test-watch \
    test-sql-tier-c-shard test-sql-tier-c-merge test-sql-tier-c-replay ci-sql-tier-b \
    version release-version check-versioning check-dependency-pins \
    release-prepared-check release-commit-check release-committed-check release-tagged-check \
    release-prepare-version install install-dev update-dev package publish \
    check-portable-automation check-invariants; do
    : > "$CI_CARGO_TRACE"
    status=0
    PATH="$native/bin:$PATH" make --no-print-directory -C "$native" -j2 "$mode" "$lane" \
      > "$native/mode-$mode-$lane.log" 2>&1 || status=$?
    [[ "$status" == 2 && ! -s "$CI_CARGO_TRACE" ]]
  done
done
for lane in ci-sql-tier-a test ci-sql-tier-b; do
  status=0
  PATH="$native/bin:$PATH" CI_CARGO_FAIL=1 \
    ICYDB_VALIDATION_FAILURE_LOG_DIR="$native/failure-logs" \
    make --no-print-directory -C "$native" -j2 "$lane" \
    "POCKET_IC_RUNNER=$native/bin/managed-run" \
    "POCKET_IC_BIN=$native/testkit-selected-server" \
    > "$native/failure-$lane.log" 2>&1 || status=$?
  [[ "$status" == 2 ]]
  rg -F native-cargo-retained-payload "$native/failure-logs/latest-combined.log" >/dev/null
done

# Stub only the network and host identity. The real shared installer verifies
# the archive bytes and tool version, then installs into this disposable fixture.
cat > "$FIXTURE/bin/uname" <<'HOST'
#!/usr/bin/env bash
case "$1" in
  -s) printf '%s\n' "$TEST_HOST" ;;
  -m) printf '%s\n' "$TEST_ARCH" ;;
  *) exit 2 ;;
esac
HOST
cat > "$FIXTURE/bin/curl" <<'DOWNLOAD'
#!/usr/bin/env bash
set -euo pipefail
output=""
request=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) output="$2"; shift 2 ;;
    *) request="$1"; shift ;;
  esac
done
printf '%s\n' "$request" > "$TEST_REQUEST"
cp "$TEST_ARCHIVE" "$output"
DOWNLOAD
cat > "$FIXTURE/bin/actionlint" <<'TOOL'
#!/usr/bin/env bash
printf '%s\n' 1.7.12
TOOL
chmod +x "$FIXTURE/bin/"*
tar -czf "$FIXTURE/artifact.tar.gz" -C "$FIXTURE/bin" actionlint
digest="$(bash "$FIXTURE/scripts/ci/verify-file-checksum.sh" --print sha256 "$FIXTURE/artifact.tar.gz")"
printf 'version\t1.7.12\n' > "$FIXTURE/scripts/ci/actionlint-checksums.tsv"
for platform in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
  printf '%s\tactionlint_1.7.12_%s.tar.gz\n' "$digest" "$platform" \
    >> "$FIXTURE/scripts/ci/actionlint-checksums.tsv"
done
for host in Linux:x86_64:linux_amd64 Linux:aarch64:linux_arm64 \
  Darwin:x86_64:darwin_amd64 Darwin:arm64:darwin_arm64; do
  os="${host%%:*}"
  remainder="${host#*:}"
  arch="${remainder%%:*}"
  platform="${remainder#*:}"
  bin="$(PATH="$FIXTURE/bin:$PATH" TEST_HOST="$os" TEST_ARCH="$arch" \
    TEST_REQUEST="$FIXTURE/request" TEST_ARCHIVE="$FIXTURE/artifact.tar.gz" \
    ACTIONLINT_INSTALL_DIR="$FIXTURE/install" \
    bash "$FIXTURE/scripts/ci/install-icydb-actionlint.sh")"
  [[ "$("$bin" -version)" == 1.7.12 ]]
  [[ "$(cat "$FIXTURE/request")" == \
    "https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_${platform}.tar.gz" ]]
done

# Exercise compiled invariant transport through the actual aggregate recipes.
# Other fixtures/static checks are substitutes; selected Cargo failures stop
# before any product gate or commit-producing receipt body can execute.
for script in "$ROOT/scripts/ci/"{check-,test-}*.sh "$ROOT/scripts/release/test-"*.sh \
  "$ROOT/scripts/ci/verify-shared-tooling-snapshot.sh"; do
  relative="${script#"$ROOT/"}"
  mkdir -p "$native/${relative%/*}"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$native/$relative"
done
mkdir -p "$native/scripts/dev"
for script in scripts/dev/delete-github-tags-up-to.pl \
  scripts/ci/test-documentation.pl scripts/ci/check-documentation.pl; do
  printf 'exit 0;\n' > "$native/$script"
done
for script in check-make-execution.sh invariant-common.sh check-dependency-graph-invariants.sh \
  check-executor-no-production-panics.sh test-invariant-scanners.sh; do
  cp "$ROOT/scripts/ci/$script" "$native/scripts/ci/"
done
cp "$ROOT/"{Cargo.toml,Cargo.lock} "$native/"
for owner in executor commit journal startup; do
  mkdir -p "$native/crates/icydb-core/src/db/$owner"
  printf 'fn production() {}\n' > "$native/crates/icydb-core/src/db/$owner/probe.rs"
done
for command in build locate-project run; do
  : > "$CI_CARGO_TRACE"
  status=0
  lane=check-invariants
  if [[ "$command" == build ]]; then lane=check-portable-automation; fi
  PATH="$native/bin:$PATH" CI_CARGO_FAIL=1 CI_CARGO_FAIL_COMMAND="$command" \
    make --no-print-directory -C "$native" -j2 "$lane" \
    > "$native/compiled-$command.log" 2>&1 || status=$?
  [[ "$status" == 2 ]]
  rg -F native-cargo-failure "$native/compiled-$command.log" >/dev/null
  if [[ "$command" == run ]]; then
    printf '%s\n' 'locate-project default' 'run default' > "$native/expected-trace"
  else printf '%s default\n' "$command" > "$native/expected-trace"; fi
  cmp "$native/expected-trace" "$CI_CARGO_TRACE"
  # Later probes select graph/runtime checks without running scanner semantics.
  printf '#!/usr/bin/env bash\nexit 0\n' > "$native/scripts/ci/test-invariant-scanners.sh"
done
echo "shared-tooling adapter regressions passed"
fixture_complete=true
