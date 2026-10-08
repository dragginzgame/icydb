#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/icydb-shared-adapters.XXXXXX")"
trap 'status=$?; if [[ "$status" == 0 ]]; then rm -rf "$FIXTURE";
  else echo "Shared adapter fixture retained: $FIXTURE" >&2; fi; exit "$status"' EXIT
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
MAKE

# Exercise the canonical runner via the adapter, retaining every raw target
# failure and both summaries. A later passing check must preserve prior evidence.
status=0
ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/logs" \
  bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" pass fail-one fail-two \
  > "$FIXTURE/output" 2>&1 || status=$?
# Make reports a failed recipe as status 2; preserve that through both owners.
[[ "$status" -eq 2 ]]
for payload in first-complete-payload second-complete-payload; do
  rg -F "$payload" "$FIXTURE/logs/latest-combined.log" > /dev/null
done
# Complete batch bytes preserve dispatch order; latest.log retains the distinct
# last-failed-target contract of the shared runner.
awk '/^first-complete-payload$/ { first = NR } /^second-complete-payload$/ { second = NR }
  END { exit !(first > 0 && second > first) }' "$FIXTURE/logs/latest-combined.log"
rg -F second-complete-payload "$FIXTURE/logs/latest.log" > /dev/null
if rg -F first-complete-payload "$FIXTURE/logs/latest.log" > /dev/null; then exit 1; fi
[[ -s "$FIXTURE/logs/latest-errors.log" ]]
for log in "$FIXTURE/logs"/*-[0-9]*-*.log; do
  [[ -s "$log" ]]
done
cp "$FIXTURE/logs/latest-combined.log" "$FIXTURE/previous.log"
ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/logs" \
  bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" pass \
  > "$FIXTURE/pass-output" 2>&1
cmp "$FIXTURE/logs/latest-combined.log" "$FIXTURE/previous.log"

# Direct adapter callers must not report evidence from an unexecuted or
# failure-suppressing Make invocation, or replace prior complete failure logs.
rm "$FIXTURE/executed"
for flags in i n q t v --ignore-errors --just-print --question --touch --version; do
  status=0
  MAKEFLAGS="$flags" ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/logs" \
    bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" pass \
    > "$FIXTURE/refused-${flags#--}" 2>&1 || status=$?
  [[ "$status" -ne 0 && ! -e "$FIXTURE/executed" ]]
  cmp "$FIXTURE/logs/latest-combined.log" "$FIXTURE/previous.log"
done

status=0
ICYDB_VALIDATION_FAILURE_LOG_DIR="$FIXTURE/fast-logs" \
  bash "$FIXTURE/scripts/ci/run-icydb-validation-targets.sh" --fail-fast fail-one fail-two \
  > "$FIXTURE/fast-output" 2>&1 || status=$?
[[ "$status" -eq 2 ]]
rg -F first-complete-payload "$FIXTURE/fast-logs/latest-combined.log" > /dev/null
if rg -F second-complete-payload "$FIXTURE/fast-logs/latest-combined.log" > /dev/null; then
  echo "fail-fast executed a later target" >&2
  exit 1
fi

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

# A digest refusal must happen before executing an untrusted candidate and must
# preserve the installed executable. No real download or global install occurs.
cp "$FIXTURE/install/actionlint" "$FIXTURE/installed-before"
cat > "$FIXTURE/bin/actionlint" <<'UNTRUSTED'
#!/usr/bin/env bash
touch "$TEST_SIDE_EFFECT"
printf '%s\n' 1.7.12
UNTRUSTED
tar -czf "$FIXTURE/untrusted.tar.gz" -C "$FIXTURE/bin" actionlint
status=0
PATH="$FIXTURE/bin:$PATH" TEST_HOST=Linux TEST_ARCH=x86_64 \
  TEST_REQUEST="$FIXTURE/request" TEST_ARCHIVE="$FIXTURE/untrusted.tar.gz" \
  TEST_SIDE_EFFECT="$FIXTURE/executed" ACTIONLINT_INSTALL_DIR="$FIXTURE/install" \
  bash "$FIXTURE/scripts/ci/install-icydb-actionlint.sh" \
  > "$FIXTURE/rejection" 2>&1 || status=$?
[[ "$status" -ne 0 && ! -e "$FIXTURE/executed" ]]
cmp "$FIXTURE/install/actionlint" "$FIXTURE/installed-before"
# Authentic bytes with the wrong version also preserve the selected executable
# and retain their failed candidate on the destination filesystem.
cat > "$FIXTURE/bin/actionlint" <<'WRONG_VERSION'
#!/usr/bin/env bash
touch "$TEST_SIDE_EFFECT"
printf '%s\n' 1.7.120
WRONG_VERSION
tar -czf "$FIXTURE/wrong-version.tar.gz" -C "$FIXTURE/bin" actionlint
digest="$(bash "$FIXTURE/scripts/ci/verify-file-checksum.sh" --print sha256 "$FIXTURE/wrong-version.tar.gz")"
printf 'version\t1.7.12\n%s\tactionlint_1.7.12_linux_amd64.tar.gz\n' "$digest" \
  > "$FIXTURE/scripts/ci/actionlint-checksums.tsv"
status=0
PATH="$FIXTURE/bin:$PATH" TEST_HOST=Linux TEST_ARCH=x86_64 \
  TEST_REQUEST="$FIXTURE/request" TEST_ARCHIVE="$FIXTURE/wrong-version.tar.gz" \
  TEST_SIDE_EFFECT="$FIXTURE/version-executed" ACTIONLINT_INSTALL_DIR="$FIXTURE/install" \
  bash "$FIXTURE/scripts/ci/install-icydb-actionlint.sh" \
  > "$FIXTURE/version-rejection" 2>&1 || status=$?
[[ "$status" -ne 0 && -f "$FIXTURE/version-executed" ]]
cmp "$FIXTURE/install/actionlint" "$FIXTURE/installed-before"
retained=0
for stage in "$FIXTURE/install"/.actionlint-install.*; do
  if [[ -f "$stage/actionlint" ]] && cmp -s "$stage/actionlint" "$FIXTURE/bin/actionlint"; then
    retained=$((retained + 1))
  fi
done
[[ "$retained" == 1 ]]
echo "shared-tooling adapter regressions passed"
