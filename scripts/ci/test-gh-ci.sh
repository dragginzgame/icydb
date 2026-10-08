#!/usr/bin/env bash
set -euo pipefail
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/gh-ci-test.XXXXXX")"
trap 'if [[ $? == 0 ]]; then rm -rf "$fixture"; else echo "CI inspection fixtures retained: $fixture" >&2; fi' EXIT
mkdir "$fixture/bin"
export GH_CI_TEST_LOG="$fixture/calls"
export GH_CI_TEST_SHA=0123456789012345678901234567890123456789
# Keep command traces out of captured Git output.
cat > "$fixture/bin/git" <<'SCRIPT'
#!/usr/bin/env bash
{ printf 'git'; printf ' <%s>' "$@"; printf '\n'; } >> "$GH_CI_TEST_LOG"
case "$1" in
    rev-parse) printf '%s\n' "$GH_CI_TEST_SHA"; exit "${GH_CI_TEST_GIT_STATUS:-0}" ;;
    branch) printf '%s\n' main ;;
    *) exit 90 ;;
esac
SCRIPT
cat > "$fixture/bin/gh" <<'SCRIPT'
#!/usr/bin/env bash
{ printf 'gh'; printf ' <%s>' "$@"; printf '\n'; } >> "$GH_CI_TEST_LOG"
case "$1 $2" in
    'auth status') exit "${GH_CI_TEST_AUTH_STATUS:-0}" ;;
    'run list')
        [[ ${GH_CI_TEST_LIST_STATUS:-0} == 0 ]] || exit "$GH_CI_TEST_LIST_STATUS"
        for arg in "$@"; do
            if [[ "$arg" == --json ]]; then printf '%s' "${GH_CI_TEST_ID-42}"; exit; fi
        done
        echo 'queued workflow evidence'
        ;;
    'run view') exit "${GH_CI_TEST_VIEW_STATUS:-0}" ;;
    *) exit 91 ;;
esac
SCRIPT
chmod +x "$fixture/bin/"*
export PATH="$fixture/bin:$PATH"
run() {
    : > "$GH_CI_TEST_LOG"
    bash "$ROOT/scripts/dev/gh-ci.sh" "$@" > "$fixture/output" 2>&1
}
refuse() {
    local expected="$1" status=0
    shift
    run "$@" || status=$?
    [[ "$status" == "$expected" ]] || { cat "$fixture/output"; exit 1; }
}

run
grep -F 'gh <run> <list> <--workflow> <CI> <--branch> <main> <--limit> <1>' "$GH_CI_TEST_LOG"
grep -Fx 'gh <run> <view> <42> <--verbose>' "$GH_CI_TEST_LOG"

run --commit HEAD --all-workflows --limit 100
grep -Fx 'git <rev-parse> <--verify> <--end-of-options> <HEAD^{commit}>' "$GH_CI_TEST_LOG"
grep -Fx "gh <run> <list> <--commit> <$GH_CI_TEST_SHA> <--limit> <100>" "$GH_CI_TEST_LOG"
grep -Fx "Selected commit: $GH_CI_TEST_SHA" "$fixture/output"
grep -Fx 'queued workflow evidence' "$fixture/output"
if grep -F 'gh <run> <view>' "$GH_CI_TEST_LOG"; then exit 1; fi
if grep -F '<--branch>' "$GH_CI_TEST_LOG"; then exit 1; fi

run --commit HEAD --branch release --workflow 'Rust checks' --failed --logs
grep -F "<--workflow> <Rust checks> <--branch> <release> <--commit> <$GH_CI_TEST_SHA> <--status> <failure>" "$GH_CI_TEST_LOG"
grep -Fx 'gh <run> <view> <42> <--log-failed>' "$GH_CI_TEST_LOG"
grep -F 'Historical failure search' "$fixture/output"

run --all-workflows --branch main
grep -Fx 'gh <run> <list> <--branch> <main> <--limit> <10>' "$GH_CI_TEST_LOG"
run --run 17 --logs
grep -Fx 'gh <run> <view> <17> <--log-failed>' "$GH_CI_TEST_LOG"
if grep -F 'gh <run> <list>' "$GH_CI_TEST_LOG"; then exit 1; fi

refuse 2 --commit HEAD --run 42
refuse 2 --all-workflows --workflow CI
refuse 2 --all-workflows --run 42
refuse 2 --all-workflows --logs
refuse 2 --limit 0
refuse 2 --commit
[[ ! -s "$GH_CI_TEST_LOG" ]]

# Partial output on failed observations must never dispatch run inspection.
GH_CI_TEST_GIT_STATUS=9 refuse 1 --commit HEAD --all-workflows
if grep -F 'gh ' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_SHA=malformed refuse 1 --commit HEAD
if grep -F 'gh ' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_AUTH_STATUS=7 refuse 1 --commit HEAD
if grep -F 'gh <run>' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_LIST_STATUS=8 refuse 8 --commit HEAD
if grep -F 'gh <run> <view>' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_ID='' refuse 1 --commit HEAD
if grep -F 'gh <run> <view>' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_VIEW_STATUS=6 refuse 6 --run 42 --logs
if grep -F '<--log-failed>' "$GH_CI_TEST_LOG"; then exit 1; fi
echo 'CI inspection selection and failure tests passed'
