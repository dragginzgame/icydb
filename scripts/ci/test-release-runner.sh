#!/usr/bin/env bash
set -euo pipefail

# This fixture owns its Make controls; production admission is tested below.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/release-runner-test.XXXXXX")"
trap 'if [[ $? == 0 ]]; then rm -rf "$FIXTURE_ROOT"; else printf "Failed release-runner fixture retained: %s\n" "$FIXTURE_ROOT" >&2; fi' EXIT
export REAL_GIT REAL_MAKE
REAL_GIT="$(command -v git)"
REAL_MAKE="$(command -v make)"
mkdir -p "$FIXTURE_ROOT/bin"

cat > "$FIXTURE_ROOT/bin/make" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
# Admission uses real GNU Make; all release effects below remain substitutes.
if [[ "${2:-}" == -f && "${3:-}" == - ]]; then exec "$REAL_MAKE" "$@"; fi
while [[ "$1" == --no-print-directory || "$1" == -s ]]; do shift; done
target="$1"
shift
for argument in "$@"; do export "$argument"; done
if [[ "$target" == release-version ]]; then cat version; exit; fi
printf '%s\n' "$target" >> events
printf '%s %s %s\n' "$target" "$RELEASE_VERSION" "${RELEASE_COMMIT:-}" >> selections
if [[ "$target" == release-verify ]]; then
    attempt="$(awk '$0 == "release-verify" { count++ } END { print count }' events)"
    printf 'validation source: %s\n' "$RELEASE_SOURCE" > "validation.$attempt.log"
    printf 'build source: %s\n' "$RELEASE_SOURCE" > "build.$attempt.evidence"
fi
[[ "${FIXTURE_FAIL_TARGET:-}" != "$target" ]] || exit 7
if [[ "${FIXTURE_DRIFT_TARGET:-}" == "$target" ]]; then
    printf '%s\n' "$FIXTURE_DRIFT_URL" > destination
fi
case "$target" in
    release-preflight) [[ "$(cat version)" == "$RELEASE_PREVIOUS" ]] ;;
    release-verify) ;;
    release-prepare-version)
        [[ "$(tail -n 1 ".release-state/$RELEASE_VERSION.plan")" == prepare ]]
        [[ "$(sed -n '6p' ".release-state/$RELEASE_VERSION.plan")" == "$RELEASE_SOURCE" ]]
        printf '%s\n' "$RELEASE_VERSION" > version
        ;;
    release-prepared-check) [[ "$(cat version)" == "$RELEASE_VERSION" ]] ;;
    release-files) printf 'version\0release file.txt\0' ;;
    release-commit-check) ;;
    release-committed-check|release-tagged-check|release-push-check)
        [[ -n "$RELEASE_COMMIT" ]]
        [[ "$(cat "commits/$RELEASE_COMMIT.subject")" == "Release $RELEASE_VERSION" ]]
        ;;
    *) exit 2 ;;
esac
STUB

cat > "$FIXTURE_ROOT/bin/git" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
release_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
tag_sha=cccccccccccccccccccccccccccccccccccccccc
resolve() {
    if [[ "$1" == HEAD ]]; then cat head; else printf '%s\n' "$1"; fi
}
ancestor() {
    local cursor
    cursor="$(resolve "$2")"
    while [[ "$cursor" != "$1" ]]; do
        [[ -f "commits/$cursor.parent" ]] || return 1
        cursor="$(cat "commits/$cursor.parent")"
    done
}
case "$1" in
    check-ref-format) [[ "$2" == refs/heads/main ]] ;;
    symbolic-ref) echo main ;;
    remote)
        if [[ -f destination ]]; then cat destination
        else printf '%s\n' "${FIXTURE_DESTINATION:-https://example.invalid/release-fixture}"; fi
        ;;
    hash-object) exec "$REAL_GIT" hash-object --stdin ;;
    rev-parse)
        case "${*: -1}" in
            --show-toplevel) pwd ;;
            release-state) echo .release-state ;;
            HEAD) cat head ;;
            HEAD^) [[ "$(cat head)" == "$release_sha" ]]; cat parent-head ;;
            *'^{tree}')
                name="${*: -1}"; name="$(resolve "${name%\^\{tree\}}")"
                if [[ -n "${FIXTURE_COMMIT_TREE:-}" ]]; then echo "$FIXTURE_COMMIT_TREE"; elif [[ -f "commits/$name.tree" ]]; then cat "commits/$name.tree"; else echo dddddddddddddddddddddddddddddddddddddddd; fi
                ;;
            refs/tags/*'^{commit}')
                name="${*: -1}"; name="${name#refs/tags/}"; name="${name%\^\{commit\}}"
                if [[ -n "${FIXTURE_TAG_COMMIT:-}" ]]; then echo "$FIXTURE_TAG_COMMIT"; elif [[ -f "tags/$name" ]]; then cat "tags/$name"; else [[ -f tag ]]; echo "$release_sha"; fi
                ;;
            refs/tags/*) [[ -f tag ]]; echo "$tag_sha" ;;
            *) exit 2 ;;
        esac
        ;;
    diff)
        if [[ "$2" == --quiet ]]; then [[ "${FIXTURE_DIRTY:-}" != yes ]]; else cat version; fi
        ;;
    ls-files)
        [[ "${FIXTURE_INVENTORY_FAIL:-}" != yes ]] || exit 9
        if [[ "${FIXTURE_UNTRACKED:-}" == yes ]]; then echo unrelated-file; fi
        ;;
    write-tree) echo "${FIXTURE_INDEX_TREE:-dddddddddddddddddddddddddddddddddddddddd}" ;;
    rev-list)
        range="${*: -1}"; base="${range%..HEAD}"
        cursor="$(cat head)"; history=""
        while [[ "$cursor" != "$base" ]]; do
            [[ -f "commits/$cursor.parent" ]] || exit 1
            history="$cursor${history:+$'\n'$history}"
            cursor="$(cat "commits/$cursor.parent")"
        done
        [[ -z "$history" ]] || printf '%s\n' "$history"
        ;;
    merge-base) ancestor "$3" "$4" ;;
    log)
        sha="${*: -1}"
        case "$3" in
            --format=%P) cat "commits/$sha.parent" ;;
            --format=%s) cat "commits/$sha.subject" ;;
            *) exit 2 ;;
        esac
        ;;
    tag)
        case "$2" in
            --list) if [[ -f tag && "$(cat tag)" == "${3#v}" ]]; then echo "$3"; fi ;;
            -a)
                [[ "$4" == "$(cat head)" || -f "commits/$4.parent" ]]
                mkdir -p tags
                printf '%s\n' "${FIXTURE_TAG_COMMIT:-$4}" > "tags/$3"
                printf '%s\n' "${3#v}" > tag
                echo tag >> events
                if [[ "${FIXTURE_FAIL_EFFECT:-}" == tag && ! -f lost-tag ]]; then touch lost-tag; exit 9; fi
                ;;
            *) exit 2 ;;
        esac
        ;;
    cat-file) [[ -f tag ]]; echo "${FIXTURE_TAG_TYPE:-tag}" ;;
    ls-remote)
        [[ "${FIXTURE_REMOTE_FAIL:-}" != yes ]] || exit 9
        if [[ "${FIXTURE_DRIFT_TARGET:-}" == remote-observation && -f tag ]]; then
            printf '%s\n' "$FIXTURE_DRIFT_URL" > destination
        fi
        for ref in "$@"; do
            case "$ref" in
                refs/heads/main) if [[ -f remote-head ]]; then printf '%s\t%s\n' "$(cat remote-head)" "$ref"; fi ;;
                refs/tags/*) if [[ -f remote-tag && "$(cat remote-tag-name)" == "${ref#refs/tags/v}" ]]; then printf '%s\t%s\n' "$(cat remote-tag)" "$ref"; fi ;;
            esac
        done
        ;;
    add)
        [[ "$#" == 4 && "$2" == -- && "$3" == version && "$4" == 'release file.txt' ]]
        echo stage >> events
        if [[ "${FIXTURE_FAIL_EFFECT:-}" == stage && ! -f lost-stage ]]; then touch lost-stage; exit 9; fi
        ;;
    commit)
        [[ "$#" == 3 && "$2" == -m && "$3" == "Release $(cat version)" ]]
        cp head parent-head
        if [[ -f "commits/$release_sha.parent" ]]; then release_sha="$(printf '%s\n' "$3" "$(cat head)" | "$REAL_GIT" hash-object --stdin)"; fi
        mkdir -p commits
        cp head "commits/$release_sha.parent"
        echo dddddddddddddddddddddddddddddddddddddddd > "commits/$release_sha.tree"
        printf '%s\n' "$3" > "commits/$release_sha.subject"
        echo "$release_sha" > head
        echo commit >> events
        if [[ "${FIXTURE_FAIL_EFFECT:-}" == commit && ! -f lost-commit ]]; then touch lost-commit; exit 9; fi
        ;;
    push)
        [[ "$#" == 7 && "$2" == --no-follow-tags && "$3" == --atomic && "$4" == -- && "$5" == https://example.invalid/release-fixture && "$6" == *:refs/heads/main && "$7" == "refs/tags/v$(cat tag):refs/tags/v$(cat tag)" ]]
        push_head="$(resolve "${6%:refs/heads/main}")"
        if [[ -f remote-head ]]; then ancestor "$(cat remote-head)" "$push_head"; fi
        printf '%s %s\n' "$push_head" "$(cat tag)" >> pushes
        echo push >> events
        [[ "${FIXTURE_FAIL_EFFECT:-}" != before-push ]] || exit 9
        echo "$push_head" > remote-head
        echo "$tag_sha" > remote-tag
        cat tag > remote-tag-name
        if [[ "${FIXTURE_FAIL_EFFECT:-}" == push && ! -f lost-push ]]; then touch lost-push; exit 9; fi
        ;;
    *) exit 2 ;;
esac
STUB
chmod +x "$FIXTURE_ROOT/bin/make" "$FIXTURE_ROOT/bin/git"
export PATH="$FIXTURE_ROOT/bin:$PATH"

new_fixture() {
    mkdir -p "$FIXTURE_ROOT/$1"
    cd "$FIXTURE_ROOT/$1"
    printf '0.1.0\n' > version
    printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' > 'head'
    printf 'retained build artifact\n' > 'release file.txt'
}
# Seed saved intent, including preparation-free records from early attempts.
early_plan() {
    local kind="$1" candidate phase="$2" destination
    candidate="$(bash "$ROOT/scripts/ci/next-release-version.sh" "$(cat version)" "$kind")"
    destination="$(git remote get-url --push --all origin | git hash-object --stdin)"
    mkdir -p .release-state
    printf '%s\n' release-plan-1 "$kind" "$(cat version)" "$candidate" 2026-10-05 \
        "$(cat head)" origin main "$destination" '' "$phase" > ".release-state/$candidate.plan"
}
count_event() { awk -v event="$1" '$0 == event { count++ } END { print count+0 }' events; }
expect_failure() {
    if bash "$ROOT/scripts/ci/run-release.sh" "$@" > output 2>&1; then
        echo 'release runner unexpectedly accepted a failing fixture' >&2
        exit 1
    fi
}

for kind in patch minor major; do
    for flags in '' i n q t v; do
        new_fixture "make-mode-$kind-${flags:-control}"
        cat > Makefile <<'MAKE'
.PHONY: release-version release-preflight release-verify release-prepare-version
release-version:
	@cat version
release-preflight:
	@echo preflight >> events
release-verify:
	@echo validation-failed >> events
	@exit 23
release-prepare-version:
	@echo preparation-started >> events
	@exit 24
MAKE
        MAKEFLAGS="$flags" RELEASE_MAKE="$REAL_MAKE" expect_failure "$kind" origin main
        [[ "$(cat version)" == 0.1.0 && ! -e .release-state/lock ]]
        plans=(.release-state/*.plan)
        [[ ! -e "${plans[0]}" ]]
        if [[ -z "$flags" ]]; then
            [[ "$(cat events)" == $'preflight\nvalidation-failed' ]]
        else
            [[ ! -e events && ! -e .release-state ]]
            rg -F 'requires recipe execution and failure propagation' output >/dev/null
        fi
        [[ ! -e tag && ! -e commits && ! -e pushes ]]
    done
done

for kind in patch minor major; do
    new_fixture "$kind"
    case "$kind" in patch) candidate=0.1.1 ;; minor) candidate=0.2.0 ;; major) candidate=1.0.0 ;; esac
    bash "$ROOT/scripts/ci/run-release.sh" "$kind" origin main > output
    [[ "$(cat version)" == "$candidate" && "$(cat tag)" == "$candidate" ]]
    awk '/^(release-verify|release-prepare-version|stage|commit|tag|push)$/ { print }' events > observed
    printf '%s\n' release-verify release-prepare-version stage commit tag push > expected
    cmp expected observed
    [[ "$(count_event commit)" == 1 && "$(count_event push)" == 1 ]]
    [[ "$(tail -n 1 ".release-state/$candidate.plan")" == complete ]]
    [[ "$(cat 'release file.txt')" == 'retained build artifact' ]]
    [[ ! -e .release-state/lock ]]
done

for kind in patch minor major; do
    for target in release-prepare-version release-prepared-check release-commit-check release-committed-check release-tagged-check release-push-check; do
        new_fixture "retry-$kind-$target"
        case "$kind" in patch) candidate=0.1.1 ;; minor) candidate=0.2.0 ;; major) candidate=1.0.0 ;; esac
        export FIXTURE_FAIL_TARGET="$target"
        expect_failure "$kind" origin main
        [[ "$(count_event push)" == 0 && ! -e .release-state/lock ]]
        head -n 9 ".release-state/$candidate.plan" > saved-identity
        cp validation.1.log saved-log
        cp build.1.evidence saved-evidence
        cp events saved-events
        unset FIXTURE_FAIL_TARGET
        # Uncommitted preparation cannot be redirected to a different increment.
        case "$target" in
            release-prepare-version|release-prepared-check|release-commit-check)
                for other_kind in patch minor major; do
                    [[ "$other_kind" == "$kind" ]] || expect_failure "$other_kind" origin main
                done
                cmp saved-events events
                ;;
        esac
        bash "$ROOT/scripts/ci/run-release.sh" "$kind" origin main > output
        head -n 9 ".release-state/$candidate.plan" > final-identity
        cmp saved-identity final-identity
        cmp saved-log validation.1.log
        cmp saved-evidence build.1.evidence
        [[ "$(count_event release-verify)" == 1 && "$(count_event commit)" == 1 && "$(count_event tag)" == 1 && "$(count_event push)" == 1 ]]
        [[ "$(cat version)" == "$candidate" && "$(tail -n 1 ".release-state/$candidate.plan")" == complete ]]
        plans=(.release-state/*.plan)
        [[ "${#plans[@]}" == 1 && ! -e .release-state/lock ]]
        case "$target" in
            release-prepare-version) [[ "$(count_event release-prepare-version)" == 2 ]] ;;
            *) [[ "$(count_event release-prepare-version)" == 1 ]] ;;
        esac
    done
done

for kind in patch minor major; do
    for target in release-preflight release-verify; do
        new_fixture "restart-$kind-$target"
        case "$kind" in patch) candidate=0.1.1 ;; minor) candidate=0.2.0 ;; major) candidate=1.0.0 ;; esac
        export FIXTURE_FAIL_TARGET="$target"
        expect_failure "$kind" origin main
        [[ ! -e ".release-state/$candidate.plan" && ! -e .release-state/lock ]]
        cp output failed-output
        if [[ "$target" == release-verify ]]; then
            cp validation.1.log failed-log
            cp build.1.evidence failed-evidence
        fi
        unset FIXTURE_FAIL_TARGET
        # Model a committed validation correction and different inputs.
        printf 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\n' > 'head'
        printf 'corrected input\n' > corrected-input
        bash "$ROOT/scripts/ci/run-release.sh" "$kind" origin main > output
        [[ "$(sed -n '6p' ".release-state/$candidate.plan")" == eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ]]
        [[ "$(count_event release-preflight)" == 3 && "$(count_event release-prepare-version)" == 1 ]]
        [[ "$(count_event commit)" == 1 && "$(count_event push)" == 1 ]]
        if [[ "$target" == release-verify ]]; then
            [[ "$(count_event release-verify)" == 2 ]]
            cmp failed-log validation.1.log
            cmp failed-evidence build.1.evidence
            [[ "$(cat validation.2.log)" == 'validation source: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ]]
        fi
        [[ -s failed-output && "$(cat 'release file.txt')" == 'retained build artifact' && "$(cat corrected-input)" == 'corrected input' ]]
        [[ ! -e .release-state/lock ]]
    done
done

for phase in preflight validate; do
    new_fixture "retained-$phase"
    printf '0.12.0\n' > version
    early_plan minor "$phase"
    cp .release-state/0.13.0.plan earlier-plan
    printf 'earlier failed log\n' > failed-log
    printf 'earlier build evidence\n' > failed-evidence
    printf 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\n' > 'head'
    expect_failure resume 0.13.0 origin main
    bash "$ROOT/scripts/ci/run-release.sh" minor origin main > output
    archives=(.release-state/0.13.0.attempt.*)
    [[ "${#archives[@]}" == 1 ]]
    cmp earlier-plan "${archives[0]}/0.13.0.plan"
    [[ "$(cat version)" == 0.13.0 && "$(count_event release-verify)" == 1 ]]
    [[ "$(cat validation.1.log)" == 'validation source: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ]]
    [[ "$(cat failed-log)" == 'earlier failed log' && "$(cat failed-evidence)" == 'earlier build evidence' ]]
done

new_fixture retained-retry-fails-validation
early_plan patch validate
cp .release-state/0.1.1.plan earlier-plan
printf 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\n' > 'head'
export FIXTURE_FAIL_TARGET=release-verify
expect_failure patch origin main
[[ ! -e .release-state/0.1.1.plan && "$(cat version)" == 0.1.0 ]]
archives=(.release-state/0.1.1.attempt.*)
cmp earlier-plan "${archives[0]}/0.1.1.plan"
cp validation.1.log failed-log
cp build.1.evidence failed-evidence
unset FIXTURE_FAIL_TARGET
printf 'ffffffffffffffffffffffffffffffffffffffff\n' > 'head'
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
[[ "$(count_event release-verify)" == 2 && "$(count_event commit)" == 1 && "$(count_event push)" == 1 ]]
cmp failed-log validation.1.log
cmp failed-evidence build.1.evidence
[[ "$(cat validation.2.log)" == 'validation source: ffffffffffffffffffffffffffffffffffffffff' ]]

new_fixture early-exact-resume
early_plan patch validate
bash "$ROOT/scripts/ci/run-release.sh" resume 0.1.1 origin main > output
[[ "$(count_event release-preflight)" == 2 && "$(count_event release-verify)" == 1 ]]

new_fixture completed-history
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
cp .release-state/0.1.1.plan completed-plan
export FIXTURE_FAIL_TARGET=release-prepared-check
expect_failure patch origin main
unset FIXTURE_FAIL_TARGET
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
cmp completed-plan .release-state/0.1.1.plan
[[ "$(cat version)" == 0.1.2 && "$(count_event commit)" == 2 && "$(count_event push)" == 2 ]]
[[ ! -e .release-state/0.1.3.plan ]]

new_fixture reselect-before-preparation
early_plan patch validate
cp .release-state/0.1.1.plan earlier-plan
bash "$ROOT/scripts/ci/run-release.sh" minor origin main > output
archives=(.release-state/0.1.1.attempt.*)
cmp earlier-plan "${archives[0]}/0.1.1.plan"
[[ "$(cat version)" == 0.2.0 && "$(count_event release-verify)" == 1 ]]

for conflict in local-tag remote-tag prepared-files version destination remote-unavailable; do
    new_fixture "restart-conflict-$conflict"
    early_plan patch validate
    cp .release-state/0.1.1.plan earlier-plan
    case "$conflict" in
        local-tag) echo 0.1.1 > tag ;;
        remote-tag) echo cccccccccccccccccccccccccccccccccccccccc > remote-tag; echo 0.1.1 > remote-tag-name ;;
        prepared-files) printf 'version\0' > .release-state/0.1.1.plan.files ;;
        version) echo 0.1.1 > version ;;
        destination) export FIXTURE_DESTINATION=https://example.invalid/other ;;
        remote-unavailable) export FIXTURE_REMOTE_FAIL=yes ;;
    esac
    expect_failure patch origin main
    cmp earlier-plan .release-state/0.1.1.plan
    [[ ! -e events && ! -e .release-state/lock ]]
    unset FIXTURE_DESTINATION
    unset FIXTURE_REMOTE_FAIL
done

for kind in patch minor major; do
    for effect in stage commit tag push before-push; do
        new_fixture "lost-$kind-$effect"
        case "$kind" in patch) candidate=0.1.1 ;; minor) candidate=0.2.0 ;; major) candidate=1.0.0 ;; esac
        export FIXTURE_FAIL_EFFECT="$effect"
        expect_failure "$kind" origin main
        unset FIXTURE_FAIL_EFFECT
        head -n 9 ".release-state/$candidate.plan" > saved-identity
        cp validation.1.log saved-log
        cp build.1.evidence saved-evidence
        bash "$ROOT/scripts/ci/run-release.sh" "$kind" origin main > output
        head -n 9 ".release-state/$candidate.plan" > final-identity
        cmp saved-identity final-identity
        cmp saved-log validation.1.log
        cmp saved-evidence build.1.evidence
        [[ "$(cat version)" == "$candidate" && "$(count_event release-verify)" == 1 ]]
        [[ "$(count_event release-prepare-version)" == 1 && "$(count_event commit)" == 1 && "$(count_event tag)" == 1 ]]
        [[ "$(tail -n 1 ".release-state/$candidate.plan")" == complete ]]
        plans=(.release-state/*.plan)
        [[ "${#plans[@]}" == 1 ]]
        case "$effect" in stage) [[ "$(count_event stage)" == 2 ]] ;; *) [[ "$(count_event stage)" == 1 ]] ;; esac
        case "$effect" in before-push) [[ "$(count_event push)" == 2 ]] ;; *) [[ "$(count_event push)" == 1 ]] ;; esac
    done
done

new_fixture late-exact-resume
export FIXTURE_FAIL_EFFECT=push
expect_failure patch origin main
unset FIXTURE_FAIL_EFFECT
bash "$ROOT/scripts/ci/run-release.sh" resume 0.1.1 origin main > output
[[ "$(count_event commit)" == 1 && "$(count_event tag)" == 1 && "$(count_event push)" == 1 ]]

# Model a maintainer commit after the saved release, without using real commits.
commit_fix() {
    local fix=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    mkdir -p commits
    cp head "commits/$fix.parent"
    printf 'Fix release callback\n' > "commits/$fix.subject"
    printf '%s\n' "$fix" > "commits/$fix.tree"
    printf '%s\n' "$fix" > 'head'
}
for next_kind in patch minor major resume; do
    for outcome in before-push push; do
        new_fixture "descendant-$next_kind-$outcome"
        export FIXTURE_FAIL_EFFECT="$outcome"
        expect_failure minor origin main
        unset FIXTURE_FAIL_EFFECT
        head -n 10 .release-state/0.2.0.plan > original-identity
        cp validation.1.log original-validation
        commit_fix
        if [[ "$next_kind" == resume ]]; then
            bash "$ROOT/scripts/ci/run-release.sh" resume 0.2.0 origin main > output
            [[ "$(cat version)" == 0.2.0 && "$(count_event commit)" == 1 && "$(count_event release-verify)" == 1 ]]
        else
            case "$next_kind" in patch) next_version=0.2.1 ;; minor) next_version=0.3.0 ;; major) next_version=1.0.0 ;; esac
            bash "$ROOT/scripts/ci/run-release.sh" "$next_kind" origin main > output
            [[ "$(cat version)" == "$next_version" && "$(count_event commit)" == 2 && "$(count_event tag)" == 2 ]]
            [[ "$(count_event release-verify)" == 2 && "$(tail -n 1 ".release-state/$next_version.plan")" == complete ]]
            [[ "$(cat validation.2.log)" == 'validation source: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ]]
        fi
        head -n 10 .release-state/0.2.0.plan > reconciled-identity
        cmp original-identity reconciled-identity
        cmp original-validation validation.1.log
        [[ "$(tail -n 1 .release-state/0.2.0.plan)" == complete ]]
        [[ "$(cat tags/v0.2.0)" == bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ]]
        # Older recovery pushes only its exact release commit, never the untested fix.
        if [[ "$outcome" == before-push ]]; then
            [[ "$(sed -n '2p' pushes)" == 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0.2.0' ]]
        elif [[ "$next_kind" == resume ]]; then
            [[ "$(count_event push)" == 1 ]]
        else
            [[ "$(count_event push)" == 2 ]]
        fi
        [[ ! -e .release-state/lock && -f 'release file.txt' ]]
    done
done

for target in release-committed-check release-tagged-check; do
    new_fixture "descendant-before-tag-$target"
    export FIXTURE_FAIL_TARGET="$target"
    expect_failure minor origin main
    unset FIXTURE_FAIL_TARGET
    commit_fix
    bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
    [[ "$(cat tags/v0.2.0)" == bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ]]
    [[ "$(count_event commit)" == 2 && "$(count_event tag)" == 2 && "$(count_event release-verify)" == 2 ]]
done

new_fixture descendant-validation-fails
export FIXTURE_FAIL_EFFECT=before-push
expect_failure minor origin main
unset FIXTURE_FAIL_EFFECT
commit_fix
export FIXTURE_FAIL_TARGET=release-verify
expect_failure patch origin main
unset FIXTURE_FAIL_TARGET
[[ "$(tail -n 1 .release-state/0.2.0.plan)" == complete && ! -e .release-state/0.2.1.plan ]]
cp validation.2.log failed-next-validation
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
cmp failed-next-validation validation.2.log
[[ "$(count_event release-verify)" == 3 && "$(count_event commit)" == 2 ]]

new_fixture remote-descendant-missing-tag
export FIXTURE_FAIL_EFFECT=before-push
expect_failure minor origin main
unset FIXTURE_FAIL_EFFECT
commit_fix
cp head remote-head
bash "$ROOT/scripts/ci/run-release.sh" resume 0.2.0 origin main > output
[[ "$(cat remote-head)" == eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ]]
[[ "$(sed -n '2p' pushes)" == 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee 0.2.0' ]]

# Use the actual Make adapter boundary. Receipt verification must inspect the
# selected historical commit, even though current HEAD contains the wiring fix.
new_fixture descendant-make-receipt
printf '0.264.10\n' > version
cat > Makefile <<'MAKE'
release-version:
	@cat version
release-preflight release-verify release-prepare-version release-prepared-check release-files release-commit-check release-committed-check:
	@"$(FIXTURE_HELPER)" $@ "RELEASE_KIND=$(RELEASE_KIND)" "RELEASE_PREVIOUS=$(RELEASE_PREVIOUS)" "RELEASE_VERSION=$(RELEASE_VERSION)" "RELEASE_DATE=$(RELEASE_DATE)" "RELEASE_SOURCE=$(RELEASE_SOURCE)" "RELEASE_COMMIT=$(RELEASE_COMMIT)"
release-tagged-check:
	@mkdir -p receipts
	@printf '%s\n' "$(RELEASE_COMMIT)" > "receipts/$(RELEASE_VERSION)"
	@"$(FIXTURE_HELPER)" $@ "RELEASE_VERSION=$(RELEASE_VERSION)" "RELEASE_COMMIT=$(RELEASE_COMMIT)"
release-push-check:
	@test "$$(git rev-parse 'refs/tags/v$(RELEASE_VERSION)^{commit}')" = "$(RELEASE_COMMIT)"
	@test "$$(cat receipts/$(RELEASE_VERSION))" = "$(RELEASE_COMMIT)"
	@"$(FIXTURE_HELPER)" $@ "RELEASE_VERSION=$(RELEASE_VERSION)" "RELEASE_COMMIT=$(RELEASE_COMMIT)"
MAKE
export RELEASE_MAKE="$REAL_MAKE" FIXTURE_HELPER="$FIXTURE_ROOT/bin/make" FIXTURE_FAIL_EFFECT=before-push
expect_failure minor origin main
unset FIXTURE_FAIL_EFFECT
commit_fix
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
[[ "$(cat receipts/0.265.0)" == bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ]]
[[ "$(cat receipts/0.265.1)" == "$(cat head)" && "$(count_event commit)" == 2 ]]
unset RELEASE_MAKE FIXTURE_HELPER

for conflict in history dirty untracked inventory tag-commit tag-type remote-tag remote-unavailable destination remote-diverged; do
    new_fixture "descendant-conflict-$conflict"
    export FIXTURE_FAIL_EFFECT=before-push
    expect_failure minor origin main
    unset FIXTURE_FAIL_EFFECT
    commit_fix
    cp .release-state/0.2.0.plan saved-plan
    cp events saved-events
    case "$conflict" in
        history) printf 'Not a release\n' > commits/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.subject ;;
        dirty) export FIXTURE_DIRTY=yes ;;
        untracked) export FIXTURE_UNTRACKED=yes ;;
        inventory) export FIXTURE_INVENTORY_FAIL=yes ;;
        tag-commit) export FIXTURE_TAG_COMMIT=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
        tag-type) export FIXTURE_TAG_TYPE=commit ;;
        remote-tag) echo dddddddddddddddddddddddddddddddddddddddd > remote-tag; echo 0.2.0 > remote-tag-name ;;
        remote-unavailable) export FIXTURE_REMOTE_FAIL=yes ;;
        destination) export FIXTURE_DESTINATION=https://example.invalid/other ;;
        remote-diverged) echo ffffffffffffffffffffffffffffffffffffffff > remote-head ;;
    esac
    expect_failure patch origin main
    cmp saved-plan .release-state/0.2.0.plan
    [[ "$(count_event commit)" == 1 && "$(count_event push)" == 1 && ! -e .release-state/0.2.1.plan ]]
    unset FIXTURE_DIRTY FIXTURE_UNTRACKED FIXTURE_INVENTORY_FAIL FIXTURE_TAG_COMMIT FIXTURE_TAG_TYPE FIXTURE_REMOTE_FAIL FIXTURE_DESTINATION
done

for conflict in source metadata index commit-tree tag-type tag-commit remote-tag remote-unavailable destination dirty untracked; do
    new_fixture "reconcile-conflict-$conflict"
    case "$conflict" in
        source|metadata) export FIXTURE_FAIL_TARGET=release-prepared-check ;;
        index) export FIXTURE_FAIL_TARGET=release-commit-check ;;
        tag-type|tag-commit) export FIXTURE_FAIL_TARGET=release-tagged-check ;;
        *) export FIXTURE_FAIL_TARGET=release-push-check ;;
    esac
    expect_failure patch origin main
    unset FIXTURE_FAIL_TARGET
    cp .release-state/0.1.1.plan saved-plan
    count_event commit > saved-commits
    count_event tag > saved-tags
    case "$conflict" in
        source) echo eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee > 'head' ;;
        metadata) echo 0.1.2 > version ;;
        index) export FIXTURE_INDEX_TREE=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
        commit-tree) export FIXTURE_COMMIT_TREE=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
        tag-type) export FIXTURE_TAG_TYPE=commit ;;
        tag-commit) export FIXTURE_TAG_COMMIT=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
        remote-tag) echo eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee > remote-tag; echo 0.1.1 > remote-tag-name ;;
        remote-unavailable) export FIXTURE_REMOTE_FAIL=yes ;;
        destination) export FIXTURE_DESTINATION=https://example.invalid/other ;;
        dirty) export FIXTURE_DIRTY=yes ;;
        untracked) export FIXTURE_UNTRACKED=yes ;;
    esac
    expect_failure patch origin main
    expect_failure resume 0.1.1 origin main
    cmp saved-plan .release-state/0.1.1.plan
    [[ "$(count_event commit)" == "$(cat saved-commits)" && "$(count_event tag)" == "$(cat saved-tags)" && "$(count_event push)" == 0 ]]
    [[ ! -e .release-state/0.1.2.plan && ! -e .release-state/lock ]]
    unset FIXTURE_INDEX_TREE FIXTURE_COMMIT_TREE FIXTURE_TAG_TYPE FIXTURE_TAG_COMMIT FIXTURE_REMOTE_FAIL FIXTURE_DESTINATION FIXTURE_DIRTY FIXTURE_UNTRACKED
done

# Destination changes within one attempt must stop before dispatch, including
# adding a second URL and changes during the final remote observation.
for drift_target in release-verify release-push-check remote-observation; do
    for drift in replaced additional; do
        new_fixture "destination-$drift_target-$drift"
        export FIXTURE_DRIFT_TARGET="$drift_target"
        export FIXTURE_DRIFT_URL=https://example.invalid/other
        if [[ "$drift" == additional ]]; then
            FIXTURE_DRIFT_URL=$'https://example.invalid/release-fixture\nhttps://example.invalid/other'
        fi
        expect_failure patch origin main
        [[ "$(count_event push)" == 0 && ! -e .release-state/lock ]]
        if [[ "$drift_target" == release-verify ]]; then
            [[ ! -e .release-state/0.1.1.plan && "$(cat version)" == 0.1.0 ]]
        else
            [[ "$(tail -n 1 .release-state/0.1.1.plan)" == push ]]
            [[ "$(count_event commit)" == 1 && "$(count_event tag)" == 1 ]]
        fi
        # Restore the exact destination; normal retry preserves recovery rules.
        unset FIXTURE_DRIFT_TARGET FIXTURE_DRIFT_URL
        rm destination
        bash "$ROOT/scripts/ci/run-release.sh" patch origin main > recovered-output
        [[ "$(count_event commit)" == 1 && "$(count_event tag)" == 1 && "$(count_event push)" == 1 ]]
        [[ -f validation.1.log && -f build.1.evidence ]]
    done
done

for second_phase in prepare validate; do
    new_fixture "competing-$second_phase"
    early_plan patch prepare
    early_plan minor "$second_phase"
    cp .release-state/0.1.1.plan saved-patch
    cp .release-state/0.2.0.plan saved-minor
    expect_failure patch origin main
    cmp saved-patch .release-state/0.1.1.plan
    cmp saved-minor .release-state/0.2.0.plan
    [[ ! -e events && ! -e .release-state/lock ]]
done

new_fixture concurrent
mkdir -p .release-state/lock
early_plan patch validate
cp .release-state/0.1.1.plan earlier-plan
expect_failure patch origin main
cmp earlier-plan .release-state/0.1.1.plan
[[ ! -e events && ! -e tag ]]

new_fixture corrupt-plan
early_plan patch validate
printf 'unexpected record\n' >> .release-state/0.1.1.plan
expect_failure resume 0.1.1 origin main
expect_failure patch origin main
[[ ! -e events ]]

new_fixture concurrent-reconciliation
export FIXTURE_FAIL_TARGET=release-prepared-check
expect_failure patch origin main
unset FIXTURE_FAIL_TARGET
mkdir -p .release-state/lock
cp .release-state/0.1.1.plan saved-plan
cp events saved-events
expect_failure patch origin main
cmp saved-plan .release-state/0.1.1.plan
cmp saved-events events

new_fixture changelog
cat > CHANGELOG.md <<'NOTES'
# Changelog

## [Draft]

- Current completed change.

## [0.1.0] - 2026-10-05

- Retained history.
NOTES
awk -v version=0.1.1 -v date=2026-10-06 -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared
sed 's/## \[Draft\]/## [0.1.1] - 2026-10-06/' CHANGELOG.md > expected
cmp expected prepared
printf '\n## [0.2.0]\n\n- Competing selection.\n' >> CHANGELOG.md
if awk -v version=0.1.1 -v date=2026-10-06 -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared 2>/dev/null; then exit 1; fi

# Consumer adapters carry the saved previous version, including after a bump.
cat > CHANGELOG.md <<'NOTES'
# Changelog

## [0.1.1]

- Current completed change.

## [0.1.0]

- Imported undated history.
NOTES
sed 's/## \[0.1.1\]/## [0.1.1] - 2026-10-06/' CHANGELOG.md > expected
awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
    -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared
cmp expected prepared
cp prepared CHANGELOG.md
awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 -v allow_finalized=1 \
    -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared
cmp expected prepared
for selected_date in 2026-10-06 2026-10-07; do
    # Reconciliation is opt-in; another date always conflicts.
    allowed=0
    [[ "$selected_date" == 2026-10-06 ]] || allowed=1
    if awk -v version=0.1.1 -v previous=0.1.0 -v date="$selected_date" -v allow_finalized="$allowed" \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared 2>/dev/null; then exit 1; fi
done

[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 patch)" == 9.8.8 ]]
[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 minor)" == 9.9.0 ]]
[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 major)" == 10.0.0 ]]
for version in 01.2.3 1.2.3-beta 1.2 9223372036854775807.0.0; do
    if bash "$ROOT/scripts/ci/next-release-version.sh" "$version" patch > /dev/null 2>&1; then exit 1; fi
done
# Preserve exact note ownership beyond binary64 precision and across lengths.
for components in '9007199254740992 9007199254740993 9007199254740991' \
    '999999999999999 1000000000000000 999999999999998'; do
    read -r previous_component candidate_component older_component <<< "$components"
    for position in major minor patch; do
        case "$position" in
            major)
                previous="$previous_component.0.0"
                candidate="$candidate_component.0.0"
                older="$older_component.0.0"
                ;;
            minor)
                previous="0.$previous_component.0"
                candidate="0.$candidate_component.0"
                older="0.$older_component.0"
                ;;
            patch)
                previous="0.0.$previous_component"
                candidate="0.0.$candidate_component"
                older="0.0.$older_component"
                ;;
        esac
        cat > CHANGELOG.md <<NOTES
# Changelog

## [$candidate]

- Pending release notes.

## [$previous]

- Equal cutoff history.

## [$older]

- Older history.
NOTES
        sed "s/## \[$candidate\]/## [$candidate] - 2026-10-06/" CHANGELOG.md > expected
        awk -v version="$candidate" -v previous="$previous" -v date=2026-10-06 \
            -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > prepared
        cmp expected prepared
        # A newer draft must not be reclassified as history at the cutoff.
        if awk -v version="$previous" -v previous="$previous" -v date=2026-10-06 \
            -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md \
            > prepared 2> conflict.log; then exit 1; fi
    done
done
# Horizontal heading whitespace is presentation, not a different release identity.
for heading in '## [Draft]' '## [0.1.1]'; do
    printf '# Changelog\n\n%s  \t\n\n- Selected notes.\n\n## [0.1.0]\t \n\n- Kept history.\n' "$heading" > whitespace-notes
    printf '# Changelog\n\n## [0.1.1] - 2026-10-06\n\n- Selected notes.\n\n## [0.1.0]\t \n\n- Kept history.\n' > whitespace-expected
    awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" whitespace-notes > whitespace-actual
    cmp whitespace-expected whitespace-actual
    printf '\n## [0.1.1]\t\n' >> whitespace-notes
    if awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" whitespace-notes > /dev/null 2>&1; then exit 1; fi
done
printf '# Changelog\n\n## [0.1.1] - 2026-10-06 \t\n\n- Finalized notes.\n' > whitespace-notes
awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 -v allow_finalized=1 \
    -f "$ROOT/scripts/ci/finalize-release-changelog.awk" whitespace-notes > whitespace-actual
cmp whitespace-notes whitespace-actual
if awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-07 -v allow_finalized=1 \
    -f "$ROOT/scripts/ci/finalize-release-changelog.awk" whitespace-notes > /dev/null 2>&1; then exit 1; fi
# Spacing around a dated identity cannot turn it into another candidate.
for selected_date in 2026-10-06 2026-10-07; do
    printf '# Changelog\n\n## [0.1.1]  - \t%s\t\n\n- Finalized.' "$selected_date" > spaced-dated
    cp spaced-dated spaced-original
    if awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" spaced-dated > spaced-result 2> spaced-error; then exit 1; fi
    [[ ! -s spaced-result ]]
    cmp spaced-dated spaced-original
    status=0
    awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 -v allow_finalized=1 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" spaced-dated > spaced-result 2> spaced-error || status=$?
    if [[ "$selected_date" == 2026-10-06 ]]; then
        [[ "$status" == 0 ]]; cmp spaced-dated spaced-result
    else
        [[ "$status" == 1 && ! -s spaced-result ]]
    fi
done

# Preserve byte boundaries at historical EOF, even when the draft moves from EOF
# to the front. A blanket removal of the output's last LF breaks the latter.
for ending in lf no-lf; do
    printf '## [0.1.0]\n\n- Historical café, literal ^$ and tabs\t.' > history-bytes
    [[ "$ending" != lf ]] || printf '\n' >> history-bytes
    printf '# Changelog\n\n## [0.1.1]\n\n- Pending.\n\n' > byte-notes
    cat history-bytes >> byte-notes
    printf '# Changelog\n\n## [0.1.1] - 2026-10-06\n\n- Pending.\n\n' > byte-expected
    cat history-bytes >> byte-expected
    awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" byte-notes > byte-result
    cmp byte-expected byte-result
    awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 -v allow_finalized=1 \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" byte-result > byte-repeated
    cmp byte-result byte-repeated
done
printf '## [0.1.0]\n\n- History.\n\n' > history-bytes
printf '# Changelog\n\n' > byte-notes
cat history-bytes >> byte-notes
printf '## [0.1.1]\n\n- Draft at EOF.' >> byte-notes
printf '# Changelog\n\n## [0.1.1] - 2026-10-06\n\n- Draft at EOF.\n' > byte-expected
cat history-bytes >> byte-expected
awk -v version=0.1.1 -v previous=0.1.0 -v date=2026-10-06 \
    -f "$ROOT/scripts/ci/finalize-release-changelog.awk" byte-notes > byte-result
cmp byte-expected byte-result
echo 'release runner command-stub tests passed'
