#!/usr/bin/env bash
# Shared companions: scripts/dev/gh-ci.sh scripts/ci/fixture-lifecycle.sh
set -euo pipefail
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/gh-ci-test.XXXXXX")"
fixture="$(CDPATH="" cd -P -- "$fixture" && printf '%s/.' "$PWD")"
fixture="${fixture%/.}"
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$ROOT/scripts/ci/fixture-lifecycle.sh"
fixture_complete=false
trap 'fixture_finish "$?" "$fixture" "CI inspection fixtures retained"' EXIT
mkdir "$fixture/bin"
export GH_CI_TEST_LOG="$fixture/calls"
export GH_CI_TEST_SHA=0123456789012345678901234567890123456789
# Keep command traces out of captured Git output.
cat > "$fixture/bin/git" <<'SCRIPT'
#!/usr/bin/env bash
{ printf 'git'; printf ' <%s>' "$@"; printf '\n'; } >> "$GH_CI_TEST_LOG"
case "$1" in
    rev-parse) printf '%s\n' "$GH_CI_TEST_SHA"; exit "${GH_CI_TEST_GIT_STATUS:-0}" ;;
    branch)
        printf '%s\n' "${GH_CI_TEST_BRANCH-main}"
        printf '%s' "${GH_CI_TEST_BRANCH_ERROR-}" >&2
        exit "${GH_CI_TEST_BRANCH_STATUS:-0}" ;;
    *) exit 90 ;;
esac
SCRIPT
cat > "$fixture/bin/gh" <<'SCRIPT'
#!/usr/bin/env bash
{ printf 'gh'; printf ' <%s>' "$@"; printf '\n'; } >> "$GH_CI_TEST_LOG"
case "$1 $2" in
    'auth status') exit "${GH_CI_TEST_AUTH_STATUS:-0}" ;;
    'repo view')
        if [[ -n "${GH_REPO:-}" ]]; then [[ "$3" == "$GH_REPO" ]] || exit 96; fi
        printf '%s\n' "${GH_CI_TEST_REPOSITORY-https://github.example/owner/selected}"
        exit "${GH_CI_TEST_REPOSITORY_STATUS:-0}" ;;
    'api --hostname')
        [[ "$3" == github.example ]] || exit 92
        case "$4" in
            repos/owner/selected/actions/runs/*/attempts/2/jobs?per_page=100)
                [[ "$5" == --paginate && "$6" == --jq ]] || exit 93
                # Exercise the actual query against pages, including successful
                # and skipped jobs that must never cause log requests.
                jq -r "$7" "$GH_CI_TEST_JOBS"
                printf '%s' "${GH_CI_TEST_JOBS_ERROR-}" >&2
                exit "${GH_CI_TEST_JOBS_STATUS:-0}" ;;
            repos/owner/selected/actions/runs/*)
                id="${4##*/}"
                printf '%s\t%s\t%s\n' "${GH_CI_TEST_RUN-$id}" "${GH_CI_TEST_ATTEMPT-2}" "${GH_CI_TEST_STATE-$'completed\tfailure'}"
                exit "${GH_CI_TEST_STATE_STATUS:-0}" ;;
            repos/owner/selected/actions/jobs/*/logs)
                case "$4" in
                    */301/logs) echo 'first failed job evidence' ;;
                    */302/logs)
                        printf '%s' "${GH_CI_TEST_JOB_TEXT-second failed job evidence}"
                        printf '%s' "${GH_CI_TEST_JOB_ERROR-}" >&2
                        exit "${GH_CI_TEST_JOB_STATUS:-0}" ;;
                    *) exit 94 ;;
                esac ;;
            *) exit 95 ;;
        esac ;;
    'run list')
        [[ ${GH_CI_TEST_LIST_STATUS:-0} == 0 ]] || exit "$GH_CI_TEST_LIST_STATUS"
        for arg in "$@"; do
            if [[ "$arg" == --json ]]; then printf '%s' "${GH_CI_TEST_ID-42}"; exit; fi
        done
        echo 'queued workflow evidence'
        ;;
    'run view')
        [[ ${GH_CI_TEST_VIEW_STATUS:-0} == 0 ]] || exit "$GH_CI_TEST_VIEW_STATUS"
        case "$4" in
            --log-failed)
                printf '%s' "${GH_CI_TEST_LOG_TEXT-failed step details}"
                printf '%s' "${GH_CI_TEST_LOG_ERROR-}" >&2
                exit "${GH_CI_TEST_LOG_STATUS:-0}" ;;
        esac
        ;;
    *) exit 91 ;;
esac
SCRIPT
chmod +x "$fixture/bin/"*
export PATH="$fixture/bin:$PATH"
export GH_CI_TEST_JOBS="$fixture/jobs.json"
printf '{"jobs":[]}\n' > "$GH_CI_TEST_JOBS"
run() {
    : > "$GH_CI_TEST_LOG"
    TMPDIR="$fixture" bash "$ROOT/scripts/dev/gh-ci.sh" "$@" > "$fixture/output" 2>&1
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

# A failed branch observation (even with partial output), or detached HEAD,
# must not silently broaden an implicit selection to unrelated branch runs.
GH_CI_TEST_BRANCH=partial GH_CI_TEST_BRANCH_STATUS=9 GH_CI_TEST_BRANCH_ERROR='branch observation failed' refuse 9
grep -F 'branch observation failed' "$fixture/output"
if grep -F 'gh ' "$GH_CI_TEST_LOG"; then exit 1; fi
for args in default list all; do
    case "$args" in
        default) GH_CI_TEST_BRANCH='' refuse 1 ;;
        list) GH_CI_TEST_BRANCH='' refuse 1 --list ;;
        all) GH_CI_TEST_BRANCH='' refuse 1 --all-workflows ;;
    esac
    grep -F 'select --branch, --commit or --run' "$fixture/output"
    if grep -F 'gh ' "$GH_CI_TEST_LOG"; then exit 1; fi
done
# Explicit identities work without observing a branch, including --run when
# invoked outside a checkout with an explicitly selected GitHub repository.
GH_CI_TEST_BRANCH_STATUS=9 run --run 17 --logs
if grep -F 'git ' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_BRANCH_STATUS=9 run --branch release --list
grep -F '<--branch> <release>' "$GH_CI_TEST_LOG"
if grep -F 'git ' "$GH_CI_TEST_LOG"; then exit 1; fi

GH_CI_TEST_BRANCH_STATUS=9 run --commit HEAD --all-workflows --limit 100
grep -Fx 'git <rev-parse> <--verify> <--end-of-options> <HEAD^{commit}>' "$GH_CI_TEST_LOG"
grep -Fx "gh <run> <list> <--commit> <$GH_CI_TEST_SHA> <--limit> <100>" "$GH_CI_TEST_LOG"
grep -Fx "Selected commit: $GH_CI_TEST_SHA" "$fixture/output"
grep -Fx 'queued workflow evidence' "$fixture/output"
if grep -F 'gh <run> <view>' "$GH_CI_TEST_LOG"; then exit 1; fi
if grep -F '<--branch>' "$GH_CI_TEST_LOG"; then exit 1; fi

run --commit HEAD --branch release --workflow 'Rust checks' --failed --logs
grep -F "<--workflow> <Rust checks> <--branch> <release> <--commit> <$GH_CI_TEST_SHA> <--status> <failure>" "$GH_CI_TEST_LOG"
grep -Fx 'gh <run> <view> <42> <--log-failed> <--repo> <github.example/owner/selected> <--attempt> <2>' "$GH_CI_TEST_LOG"
grep -F 'Historical failure search' "$fixture/output"

run --all-workflows --branch main
grep -Fx 'gh <run> <list> <--branch> <main> <--limit> <10>' "$GH_CI_TEST_LOG"
run --run 17 --logs
grep -Fx 'gh <run> <view> <17> <--log-failed> <--repo> <github.example/owner/selected> <--attempt> <2>' "$GH_CI_TEST_LOG"
grep -F 'failed step details' "$fixture/output"
if grep -F 'gh <run> <list>' "$GH_CI_TEST_LOG"; then exit 1; fi
if grep -F '/jobs?' "$GH_CI_TEST_LOG"; then exit 1; fi

refuse 2 --commit HEAD --run 42
refuse 2 --all-workflows --workflow CI
refuse 2 --all-workflows --run 42
refuse 2 --all-workflows --logs
refuse 2 --limit 0
refuse 2 --commit
[[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1

# Invalid explicit selections must not fall back, ignore filters or become CLI
# flags. Refuse them before either local Git or authenticated GitHub probes.
for selector in --branch --workflow --run; do
    refuse 2 "$selector" ''
    [[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
    refuse 2 "$selector"
    [[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
done
for id in 0 -1 1.5 abc --help $'17\n18'; do
    refuse 2 --run "$id"
    [[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
done
for selector in --list --failed; do
    refuse 2 --run 17 "$selector"
    [[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
done
for selector in --branch --workflow; do
    refuse 2 --run 17 "$selector" main
    [[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
done
refuse 2 --list --logs
[[ ! -s "$GH_CI_TEST_LOG" ]] || exit 1
run --run 17
grep -Fx 'gh <run> <view> <17> <--verbose>' "$GH_CI_TEST_LOG"
if grep -E 'git |gh <run> <list>' "$GH_CI_TEST_LOG"; then exit 1; fi
for id in 0 abc --help $'17\n18'; do
    GH_CI_TEST_ID="$id" refuse 1 --branch main
    grep -F 'invalid run identity' "$fixture/output"
    if grep -F 'gh <run> <view>' "$GH_CI_TEST_LOG"; then exit 1; fi
done

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

# Empty successful CLI output is not failed-step evidence. A genuinely successful
# run has no failed-step logs; a failed, unfinished or unknown run needs an
# explicit unavailable-evidence result rather than silent inspection success.
GH_CI_TEST_LOG_TEXT='' refuse 1 --run 42 --logs
grep -F 'failed-step logs unavailable' "$fixture/output"
for conclusion in success neutral skipped; do
    GH_CI_TEST_LOG_TEXT=$' \n\t' GH_CI_TEST_STATE=$'completed\t'"$conclusion" run --run 42 --logs
    grep -F 'No failed-step logs' "$fixture/output"
    if grep -F '/jobs?' "$GH_CI_TEST_LOG"; then exit 1; fi
done
for state in $'in_progress\t' $'queued\t' $'completed\tcancelled' malformed; do
    GH_CI_TEST_LOG_TEXT='' GH_CI_TEST_STATE="$state" refuse 1 --run 42 --logs
    if grep -F '/jobs?' "$GH_CI_TEST_LOG"; then exit 1; fi
done
GH_CI_TEST_LOG_TEXT='' GH_CI_TEST_STATE_STATUS=8 refuse 8 --run 42 --logs
GH_CI_TEST_LOG_TEXT='partial failure log' GH_CI_TEST_LOG_ERROR='fetch interrupted' \
    GH_CI_TEST_LOG_STATUS=9 refuse 9 --run 42 --logs
grep -F 'partial failure log' "$fixture/output"
retained="$(sed -n 's/^CI log observation retained: //p' "$fixture/output")"
[[ -d "$retained" && "$(cat "$retained/failed.log")" == 'partial failure log' &&
   "$(cat "$retained/errors.log")" == 'fetch interrupted' ]] || exit 1
if grep -F '/jobs?' "$GH_CI_TEST_LOG"; then exit 1; fi

# Partial or invalid identity observations cannot request logs.
GH_CI_TEST_REPOSITORY_STATUS=7 refuse 7 --run 42 --logs
GH_CI_TEST_REPOSITORY='https://github.example/other/repo/extra' refuse 1 --run 42 --logs
GH_CI_TEST_ATTEMPT=bad refuse 1 --run 42 --logs
GH_CI_TEST_RUN=99 refuse 1 --run 42 --logs
if grep -F '<--log-failed>' "$GH_CI_TEST_LOG"; then exit 1; fi

# Two API pages: only the failed/timed-out jobs of the pinned attempt may be
# downloaded. GH_REPO differs deliberately from the resolved repository.
cat > "$GH_CI_TEST_JOBS" <<'JSON'
{"jobs":[{"id":301,"run_id":42,"run_attempt":2,"conclusion":"failure"},{"id":399,"run_id":42,"run_attempt":1,"conclusion":"success"}]}
{"jobs":[{"id":302,"run_id":42,"run_attempt":2,"conclusion":"timed_out"},{"id":398,"run_id":42,"run_attempt":2,"conclusion":"skipped"}]}
JSON
GH_REPO=other/checkout GH_CI_TEST_LOG_TEXT='' run --run 42 --logs
grep -Fx 'gh <run> <view> <42> <--verbose> <--repo> <github.example/owner/selected> <--attempt> <2>' "$GH_CI_TEST_LOG"
grep -F '/attempts/2/jobs?per_page=100> <--paginate>' "$GH_CI_TEST_LOG"
[[ "$(grep -c '/logs>' "$GH_CI_TEST_LOG")" == 2 ]] || exit 1
grep -F 'first failed job evidence' "$fixture/output"
grep -F 'second failed job evidence' "$fixture/output"
grep -F 'run 42 attempt 2 job 302' "$fixture/output"
retained="$(sed -n 's/^CI log observation retained: //p' "$fixture/output")"
[[ -f "$retained/failed.log" && ! -s "$retained/failed.log" && -s "$retained/job-301.log" && -s "$retained/job-302.log" ]] || exit 1

# Stop on API errors even when earlier pages/jobs produced usable evidence.
GH_CI_TEST_LOG_TEXT='' GH_CI_TEST_JOBS_STATUS=8 GH_CI_TEST_JOBS_ERROR='second page unavailable' refuse 8 --run 42 --logs
grep -F 'second page unavailable' "$fixture/output"
if grep -F '/logs>' "$GH_CI_TEST_LOG"; then exit 1; fi
GH_CI_TEST_LOG_TEXT='' GH_CI_TEST_JOB_TEXT='partial second job' GH_CI_TEST_JOB_ERROR='job fetch failed' GH_CI_TEST_JOB_STATUS=9 refuse 9 --run 42 --logs
retained="$(sed -n 's/^CI log observation retained: //p' "$fixture/output")"
[[ -s "$retained/job-301.log" && "$(cat "$retained/job-302.log")" == 'partial second job' &&
   "$(cat "$retained/job-302.log.errors")" == 'job fetch failed' ]] || exit 1
GH_CI_TEST_LOG_TEXT='' GH_CI_TEST_JOB_TEXT=$' \n\t' refuse 1 --run 42 --logs
grep -F 'failed-job logs unavailable for job 302' "$fixture/output"

for identity in '99,2,302' '42,1,302' '42,2,"../302"'; do
    printf '{"jobs":[{"run_id":%s,"run_attempt":%s,"id":%s,"conclusion":"failure"}]}\n' \
        "${identity%%,*}" "$(printf '%s' "$identity" | cut -d, -f2)" "${identity##*,}" > "$GH_CI_TEST_JOBS"
    GH_CI_TEST_LOG_TEXT='' refuse 1 --run 42 --logs
    if grep -F '/logs>' "$GH_CI_TEST_LOG"; then exit 1; fi
done
echo 'CI inspection selection and failure tests passed'
fixture_complete=true
