#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/release-runner-test.XXXXXX")"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT
export REAL_GIT
REAL_GIT="$(command -v git)"
mkdir -p "$FIXTURE_ROOT/bin"

cat > "$FIXTURE_ROOT/bin/make" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
while [[ "$1" == --no-print-directory || "$1" == -s ]]; do shift; done
target="$1"
shift
for argument in "$@"; do export "$argument"; done
if [[ "$target" == release-version ]]; then cat version; exit; fi
printf '%s\n' "$target" >> events
if [[ "$target" == release-verify ]]; then
    attempt="$(awk '$0 == "release-verify" { count++ } END { print count }' events)"
    printf 'validation source: %s\n' "$RELEASE_SOURCE" > "validation.$attempt.log"
    printf 'build source: %s\n' "$RELEASE_SOURCE" > "build.$attempt.evidence"
fi
[[ "${FIXTURE_FAIL_TARGET:-}" != "$target" ]] || exit 7
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
    release-commit-check|release-committed-check|release-tagged-check|release-push-check) ;;
    *) exit 2 ;;
esac
STUB

cat > "$FIXTURE_ROOT/bin/git" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
release_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
tag_sha=cccccccccccccccccccccccccccccccccccccccc
case "$1" in
    check-ref-format) [[ "$2" == refs/heads/main ]] ;;
    symbolic-ref) echo main ;;
    remote) printf '%s\n' "${FIXTURE_DESTINATION:-https://example.invalid/release-fixture}" ;;
    hash-object) exec "$REAL_GIT" hash-object --stdin ;;
    rev-parse)
        case "${*: -1}" in
            --show-toplevel) pwd ;;
            release-state) echo .release-state ;;
            HEAD) cat head ;;
            HEAD^) [[ "$(cat head)" == "$release_sha" ]]; cat parent-head ;;
            'HEAD^{tree}') echo dddddddddddddddddddddddddddddddddddddddd ;;
            refs/tags/*'^{commit}') [[ -f tag ]]; echo "$release_sha" ;;
            refs/tags/*) [[ -f tag ]]; echo "$tag_sha" ;;
            *) exit 2 ;;
        esac
        ;;
    diff) if [[ "$2" != --quiet ]]; then cat version; fi ;;
    ls-files) ;;
    write-tree) echo dddddddddddddddddddddddddddddddddddddddd ;;
    log) printf 'Release %s\n' "$(cat version)" ;;
    tag)
        case "$2" in
            --list) if [[ -f tag && "$(cat tag)" == "${3#v}" ]]; then echo "$3"; fi ;;
            -a)
                printf '%s\n' "${3#v}" > tag
                echo tag >> events
                if [[ "${FIXTURE_FAIL_EFFECT:-}" == tag && ! -f lost-tag ]]; then touch lost-tag; exit 9; fi
                ;;
            *) exit 2 ;;
        esac
        ;;
    cat-file) [[ -f tag ]]; echo tag ;;
    ls-remote)
        [[ "${FIXTURE_REMOTE_FAIL:-}" != yes ]] || exit 9
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
        ;;
    commit)
        [[ "$#" == 3 && "$2" == -m && "$3" == "Release $(cat version)" ]]
        cp head parent-head
        echo "$release_sha" > head
        echo commit >> events
        if [[ "${FIXTURE_FAIL_EFFECT:-}" == commit && ! -f lost-commit ]]; then touch lost-commit; exit 9; fi
        ;;
    push)
        [[ "$#" == 6 && "$2" == --no-follow-tags && "$3" == --atomic && "$4" == origin && "$5" == HEAD:refs/heads/main && "$6" == "refs/tags/v$(cat version):refs/tags/v$(cat version)" ]]
        echo push >> events
        [[ "${FIXTURE_FAIL_EFFECT:-}" != before-push ]] || exit 9
        echo "$release_sha" > remote-head
        echo "$tag_sha" > remote-tag
        cat version > remote-tag-name
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
# Persist a preparation-free record as produced by an interrupted early attempt.
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

for target in release-prepare-version release-prepared-check release-commit-check release-committed-check release-tagged-check release-push-check; do
    new_fixture "$target"
    export FIXTURE_FAIL_TARGET="$target"
    expect_failure patch origin main
    [[ "$(count_event push)" == 0 && ! -e .release-state/lock ]]
    case "$target" in
        release-prepare-version) [[ "$(cat version)" == 0.1.0 ]] ;;
    esac
    unset FIXTURE_FAIL_TARGET
    bash "$ROOT/scripts/ci/run-release.sh" resume 0.1.1 origin main > output
    [[ "$(count_event commit)" == 1 && "$(count_event push)" == 1 ]]
    [[ "$(cat version)" == 0.1.1 ]]
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
bash "$ROOT/scripts/ci/run-release.sh" patch origin main > output
cmp completed-plan .release-state/0.1.1.plan
[[ "$(cat version)" == 0.1.2 && "$(count_event commit)" == 2 && "$(count_event push)" == 2 ]]

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

for target in release-prepare-version release-prepared-check; do
    new_fixture "restart-after-$target"
    export FIXTURE_FAIL_TARGET="$target"
    expect_failure patch origin main
    unset FIXTURE_FAIL_TARGET
    cp events earlier-events
    for kind in patch minor major; do expect_failure "$kind" origin main; done
    cmp earlier-events events
    [[ ! -e .release-state/0.1.2.plan ]]
    bash "$ROOT/scripts/ci/run-release.sh" resume 0.1.1 origin main > output
    [[ "$(count_event commit)" == 1 && "$(count_event push)" == 1 ]]
done

for effect in commit tag push before-push; do
    new_fixture "lost-$effect"
    export FIXTURE_FAIL_EFFECT="$effect"
    expect_failure patch origin main
    unset FIXTURE_FAIL_EFFECT
    cp events earlier-events
    expect_failure patch origin main
    cmp earlier-events events
    [[ ! -e .release-state/0.1.2.plan ]]
    bash "$ROOT/scripts/ci/run-release.sh" resume 0.1.1 origin main > output
    [[ "$(count_event release-prepare-version)" == 1 && "$(count_event commit)" == 1 ]]
    case "$effect" in before-push) [[ "$(count_event push)" == 2 ]] ;; *) [[ "$(count_event push)" == 1 ]] ;; esac
done

new_fixture destination-change
export FIXTURE_FAIL_EFFECT=before-push
expect_failure patch origin main
unset FIXTURE_FAIL_EFFECT
export FIXTURE_DESTINATION=https://example.invalid/other
expect_failure resume 0.1.1 origin main
[[ "$(count_event push)" == 1 ]]
unset FIXTURE_DESTINATION

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

[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 patch)" == 9.8.8 ]]
[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 minor)" == 9.9.0 ]]
[[ "$(bash "$ROOT/scripts/ci/next-release-version.sh" 9.8.7 major)" == 10.0.0 ]]
for version in 01.2.3 1.2.3-beta 1.2 9223372036854775807.0.0; do
    if bash "$ROOT/scripts/ci/next-release-version.sh" "$version" patch > /dev/null 2>&1; then exit 1; fi
done
echo 'release runner command-stub tests passed'
