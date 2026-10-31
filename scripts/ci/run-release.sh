#!/usr/bin/env bash
# Shared companions: scripts/ci/check-make-execution.sh scripts/ci/check-release-tag.sh scripts/ci/validate-release-inputs.pl scripts/ci/cargo-release-group.sh
set -euo pipefail

# Canonical ordering and Git effects; consumer targets own metadata and gates.
# Dependencies: Bash 3.2, GNU Make, Git, Perl, date and standard Unix file utilities.
# Journals live in the repository's Git directory, outside build artifacts.
usage() {
    echo 'usage: run-release.sh patch|minor|major REMOTE BRANCH' >&2
    echo '       run-release.sh resume X.Y.Z REMOTE BRANCH' >&2
    exit 2
}
fail() { echo "release refused: $1" >&2; exit 1; }
runner_dir="${BASH_SOURCE[0]}"
[[ "$runner_dir" == /* ]] || runner_dir="$PWD/$runner_dir"
runner_dir="$(cd -P "${runner_dir%/*}" && printf '%s/.' "$PWD")"
runner_dir="${runner_dir%/.}"
release_group="${RELEASE_GROUP:-}"
[[ -z "$release_group" || "$release_group" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || fail 'invalid release group'
export RELEASE_GROUP="$release_group"
plan_prefix="${release_group:+$release_group-}"
tag_prefix="${release_group:+$release_group-}v"
release_label="Release${release_group:+ $release_group}"
delivery="${RELEASE_DELIVERY:-direct}"
case "$delivery" in direct|pr) ;; *) fail 'RELEASE_DELIVERY must be direct or pr' ;; esac
if [[ "$delivery" == pr ]]; then
    for context in GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES; do
        [[ -z "${!context:-}" ]] || fail "PR delivery requires ordinary checkout context; unset $context"
    done
fi
mode="${1:-}"
if [[ "$mode" == resume ]]; then
    [[ $# -eq 4 ]] || usage
    requested_version="$2"
    shift
else
    [[ $# -eq 3 ]] || usage
    case "$mode" in patch|minor|major) ;; *) usage ;; esac
fi
remote="$2"
branch="$3"
[[ "$remote" =~ ^[A-Za-z0-9._-]+$ ]] || usage
make_bin="${RELEASE_MAKE:-make}"
bash "$(dirname "${BASH_SOURCE[0]}")/check-make-execution.sh" "$make_bin"
git check-ref-format "refs/heads/$branch" >/dev/null
root="$(git rev-parse --show-toplevel)"
cd "$root"
if [[ "$delivery" == direct ]]; then
    [[ "$(git symbolic-ref --quiet --short HEAD)" == "$branch" ]] || fail 'selected branch is not checked out'
fi
destination="$(git remote get-url --push --all "$remote")"
[[ -n "$destination" && "$destination" != *$'\n'* ]] || fail 'release requires exactly one push URL'
remote_identity="$(printf '%s\n' "$destination" | git hash-object --stdin)"
assert_destination() {
    local current_destination
    current_destination="$(git remote get-url --push --all "$remote")" || fail 'cannot inspect release destination'
    [[ "$current_destination" == "$destination" ]] || fail 'release destination changed'
}
state_root="$(git rev-parse --git-path release-state)"
if [[ "$delivery" == pr ]]; then state_root="$(git rev-parse --git-common-dir)/release-state"; fi
[[ ! -L "$state_root" ]] || fail 'release state directory is symlinked'
mkdir -p "$state_root"
if mkdir "$state_root/lock"; then
    :
else
    lock_status=$?
    if [[ -e "$state_root/lock" || -L "$state_root/lock" ]]; then
        echo 'release refused: release lock is occupied; inspect its owner before clearing a stale lock' >&2
    else
        echo 'release refused: unable to acquire release lock' >&2
    fi
    exit "$lock_status"
fi
trap 'rmdir "$state_root/lock"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$$" > "$state_root/lock/owner"
trap 'rm -f "$state_root/lock/owner"; rmdir "$state_root/lock"' EXIT

hook() (
    if [[ -n "${release_worktree:-}" ]]; then cd "$release_worktree"; fi
    "$make_bin" --no-print-directory "$@" \
        "RELEASE_KIND=$kind" "RELEASE_PREVIOUS=$previous" \
        "RELEASE_VERSION=$candidate" "RELEASE_DATE=$release_date" \
        "RELEASE_REMOTE=$remote" "RELEASE_BRANCH=$branch" "RELEASE_SOURCE=${validation_source:-$source}" \
        "RELEASE_COMMIT=${release_commit:-}" "RELEASE_DELIVERY=$delivery" "RELEASE_GROUP=$release_group" \
        "RELEASE_PREPARATION_SOURCE=$source" "RELEASE_PREPARED_COMMIT=${prepared_commit:-}"
)
read_plan() {
    [[ -f "$1" && ! -L "$1" ]] || fail 'release plan is missing or symlinked'
    {
        if ! {
            IFS= read -r saved_schema &&
            IFS= read -r saved_kind &&
            IFS= read -r saved_previous &&
            IFS= read -r saved_candidate &&
            IFS= read -r saved_date &&
            IFS= read -r saved_source &&
            IFS= read -r saved_remote &&
            IFS= read -r saved_branch &&
            IFS= read -r saved_destination &&
            IFS= read -r saved_tree &&
            IFS= read -r saved_phase
        }; then
            fail 'release plan is truncated'
        fi
        saved_group=""
        case "$saved_schema" in release-group-plan-1|release-group-pr-plan-1)
            IFS= read -r saved_group || fail 'release group record is truncated' ;;
        esac
        [[ "$saved_group" == "$release_group" ]] || fail 'release group changed'
        extra=""
        if IFS= read -r extra || [[ -n "$extra" ]]; then fail 'release plan has extra records'; fi
    } < "$1"
    case "$saved_schema" in
        release-plan-1|release-group-plan-1) saved_delivery=direct ;;
        release-pr-plan-1|release-group-pr-plan-1) saved_delivery='pr' ;;
        *) fail 'release plan identity is invalid' ;;
    esac
    perl "$runner_dir/validate-release-inputs.pl" version "$saved_candidate" >/dev/null || fail 'release plan identity is invalid'
    [[ "$1" == "$state_root/${plan_prefix}$saved_candidate.plan" ]] || fail 'release plan identity is invalid'
    [[ "$saved_source" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ && "$saved_destination" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || fail 'release plan source/date/destination is invalid'
    perl "$runner_dir/validate-release-inputs.pl" date "$saved_date" >/dev/null || fail "release plan date is invalid"
    [[ "$(perl "$(dirname "${BASH_SOURCE[0]}")/validate-release-inputs.pl" next-version "$saved_previous" "$saved_kind")" == "$saved_candidate" ]] || fail 'release plan increment is invalid'
    case "$saved_phase" in
        preflight|validate|prepare|stage|commit|complete) ;;
        tag|push) [[ "$saved_delivery" == direct ]] || fail 'release phase conflicts with delivery' ;;
        pr-publish|pr-review|pr-validate|pr-tag|pr-push) [[ "$saved_delivery" == pr ]] || fail 'release phase conflicts with delivery' ;;
        *) fail 'release plan phase is invalid' ;;
    esac
}
load_plan() {
    read_plan "$plan"
    [[ "$delivery" == "$saved_delivery" ]] || fail 'release delivery changed; reconcile the saved delivery first'
    [[ "$remote" == "$saved_remote" && "$branch" == "$saved_branch" && "$remote_identity" == "$saved_destination" ]] || fail 'release destination changed'
    kind="$saved_kind"
    previous="$saved_previous"
    candidate="$saved_candidate"
    release_date="$saved_date"
    source="$saved_source"
    index_tree="$saved_tree"
    phase="$saved_phase"
    # Early plans have no reusable validation: repeat preflight and the full gate.
    case "$phase" in preflight|validate) phase=preflight ;; esac
}
select_release() {
    release_commit=""
    prepared_commit=""
    release_worktree=""
    validation_source=""
    if [[ "$mode" == resume ]]; then
        perl "$runner_dir/validate-release-inputs.pl" version "$requested_version" >/dev/null || usage
        plan="$state_root/${plan_prefix}$requested_version.plan"
        load_plan
        return
    fi

    # Normal commands own a fresh attempt from current committed metadata.
    # Historical journals are never admission inputs or instructions to replay.
    git diff --cached --quiet HEAD -- || fail 'commit or restore staged changes before starting a release'
    git diff --quiet HEAD -- || fail 'commit or restore worktree changes before starting a release'
    untracked="$(git ls-files --others --exclude-standard)" || fail 'cannot inventory release source'
    [[ -z "$untracked" ]] || fail 'commit or remove untracked source before starting a release'
    if [[ -e ci/cargo-release-groups.json || -L ci/cargo-release-groups.json ]]; then
        [[ -n "$release_group" ]] || fail 'grouped workspace requires explicit RELEASE_GROUP'
    elif [[ -n "$release_group" ]]; then
        fail 'release group requires ci/cargo-release-groups.json'
    fi
    kind="$mode"
    previous="$("$make_bin" --no-print-directory -s release-version "RELEASE_GROUP=$release_group")"
    if [[ -n "$release_group" ]]; then
        [[ "$previous" == "$(bash "$runner_dir/cargo-release-group.sh" version "$release_group")" ]] || fail 'adapter selected the wrong group version'
    fi
    candidate="$(perl "$runner_dir/validate-release-inputs.pl" next-version "$previous" "$kind")"
    release_date="$(date -u +%F)"
    perl "$runner_dir/validate-release-inputs.pl" date "$release_date" >/dev/null || fail 'release date is invalid'
    source="$(git rev-parse --verify HEAD)"
    plan="$state_root/${plan_prefix}$candidate.plan"
    # Supersede this candidate's old local intent, including stale staging/PR
    # receipts. Removing a link removes only the link, never its target. Other
    # versions and build/validation evidence are not touched or inspected.
    rm -rf -- "$plan" "$plan.files" "$plan.pr.json"
    index_tree=""
    phase=preflight
}

save_phase() {
    phase="$1"
    temporary_plan="$(mktemp "$plan.tmp.XXXXXX")"
    schema=release-plan-1
    [[ "$delivery" != pr ]] || schema=release-pr-plan-1
    if [[ -n "$release_group" ]]; then
        schema=release-group-plan-1
        [[ "$delivery" != pr ]] || schema=release-group-pr-plan-1
    fi
    printf '%s\n' "$schema" "$kind" "$previous" "$candidate" "$release_date" \
        "$source" "$remote" "$branch" "$remote_identity" "$index_tree" "$phase" > "$temporary_plan"
    if [[ -n "$release_group" ]]; then printf '%s\n' "$release_group" >> "$temporary_plan"; fi
    mv "$temporary_plan" "$plan"
}
assert_source() {
    local current_source
    current_source="$(git rev-parse HEAD)" || fail 'cannot inspect current release source'
    [[ "$current_source" == "$source" ]] && return 0
    printf 'Release source conflict:\n  plan: %s\n  phase: %s\n  version: %s -> %s\n  saved source: %s\n  current source: %s\n' \
        "$plan" "$phase" "$previous" "$candidate" "$source" "$current_source" >&2
    echo 'Use a normal release target from clean committed source to start fresh, or restore the exact saved source for release-resume.' >&2
    fail 'validated source commit changed; saved validation cannot qualify the current source'
}
assert_release_commit() {
    # The first direct descendant must be the exact staged release. Later fixes
    # may be committed without changing its identity or repeating its commit.
    if [[ -z "$release_commit" ]]; then
        history="$(git rev-list --first-parent --reverse "$source..HEAD")"
        release_commit="${history%%$'\n'*}"
    fi
    [[ "$release_commit" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ && "$(git log -1 --format=%P "$release_commit")" == "$source" && "$(git log -1 --format=%s "$release_commit")" == "$release_label $candidate" ]] || fail 'release commit does not match the saved source/version'
    [[ "$index_tree" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ && "$(git rev-parse "$release_commit^{tree}")" == "$index_tree" ]] || fail 'release commit tree differs from the exact staged release'
    git merge-base --is-ancestor "$release_commit" HEAD || fail 'release commit is not an ancestor of the selected branch'
    [[ "$("$make_bin" --no-print-directory -s release-version "RELEASE_GROUP=$release_group")" == "$candidate" ]] || fail 'release metadata no longer matches the candidate'
    git diff --cached --quiet HEAD -- || fail 'release index differs from the committed payload'
    git diff --quiet HEAD -- || fail 'release worktree differs from the committed payload'
    untracked="$(git ls-files --others --exclude-standard)" || fail 'cannot inventory untracked release source'
    [[ -z "$untracked" ]] || fail 'release worktree has untracked source'
}
assert_release_tag() {
    bash "$runner_dir/check-release-tag.sh" "$release_commit" "$candidate" "$release_group" || fail 'existing tag differs from the exact release commit'
}
read_remote_refs() {
    refs="$(git ls-remote --refs -- "$destination" "refs/heads/$branch" "refs/tags/${tag_prefix}$candidate")"
    remote_head=""
    remote_tag=""
    while read -r sha ref extra; do
        [[ -n "$sha" || -n "$ref" ]] || continue
        [[ "$sha" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ && -z "$extra" ]] || fail 'remote ref output is malformed'
        case "$ref" in
            "refs/heads/$branch") [[ -z "$remote_head" ]] || fail 'remote branch is duplicated'; remote_head="$sha" ;;
            "refs/tags/${tag_prefix}$candidate") [[ -z "$remote_tag" ]] || fail 'remote tag is duplicated'; remote_tag="$sha" ;;
            *) fail 'remote ref output contains an unexpected identity' ;;
        esac
    done <<< "$refs"
}
# URL-form delivery deliberately bypasses mutable remote-name resolution. Git
# does not refresh that remote's tracking ref, so carry the confirmed observation
# back to the matching configured upstream without another network operation.
refresh_release_tracking() {
    local upstream tracking_remote tracking_source tracking_ref tracking_head
    local fetch_destination current_upstream
    if ! upstream="$(git for-each-ref --format='%(upstream:remotename)%09%(upstream:remoteref)%09%(upstream)' -- "refs/heads/$branch")"; then
        echo 'release delivered; local upstream could not be inspected; fetch to refresh Git status' >&2
        return 0
    fi
    IFS=$'\t' read -r tracking_remote tracking_source tracking_ref <<< "$upstream"
    [[ "$tracking_remote" == "$remote" && "$tracking_source" == "refs/heads/$branch" && "$tracking_ref" == refs/remotes/* ]] || return 0
    if ! fetch_destination="$(git remote get-url --all "$remote")" || [[ "$fetch_destination" != "$destination" ]]; then
        echo 'release delivered; upstream fetch destination differs; fetch to refresh Git status' >&2
        return 0
    fi
    if git symbolic-ref --quiet "$tracking_ref" >/dev/null 2>&1; then
        echo 'release delivered; symbolic tracking ref preserved; fetch to refresh Git status' >&2
        return 0
    fi
    if tracking_head="$(git show-ref --verify --hash "$tracking_ref" 2>/dev/null)"; then
        [[ "$tracking_head" != "$remote_head" ]] || return 0
        if ! git merge-base --is-ancestor "$tracking_head" "$remote_head"; then
            echo 'release delivered; newer or divergent tracking ref preserved; fetch to reconcile Git status' >&2
            return 0
        fi
    else
        tracking_head="${remote_head//[0-9a-f]/0}"
    fi
    # A changed mapping/destination or a concurrent ref update is not permission
    # to overwrite another local observation or repeat an already delivered push.
    current_upstream="$(git for-each-ref --format='%(upstream:remotename)%09%(upstream:remoteref)%09%(upstream)' -- "refs/heads/$branch")" || return 0
    [[ "$current_upstream" == "$upstream" && "$(git remote get-url --all "$remote")" == "$fetch_destination" ]] || return 0
    assert_destination
    # An OID comparison alone also accepts a symbolic ref resolving to that OID.
    # Prepare first, then inspect its type while Git holds the update lock.
    if ! perl - "$tracking_ref" "$remote_head" "$tracking_head" "release: observed $remote/$branch" <<'PERL'
use strict;
use warnings;
use IPC::Open2;
my ($ref, $new, $old, $message) = @ARGV;
$SIG{PIPE} = 'IGNORE';
my $pid = open2(my $reply, my $request, 'git', 'update-ref', '--no-deref', '-m', $message, '--stdin');
my $ok = eval {
    print {$request} "start\nupdate $ref $new $old\nprepare\n" or die "cannot prepare tracking update\n";
    for my $expected ("start: ok\n", "prepare: ok\n") {
        my $line = <$reply>;
        defined($line) && $line eq $expected or die "tracking transaction was not prepared\n";
    }
    open my $kind, '-|', 'git', 'symbolic-ref', '--quiet', $ref or die "cannot inspect locked tracking ref: $!\n";
    { local $/; <$kind>; }
    close $kind;
    $? == 256 or die "locked tracking ref is symbolic or could not be inspected\n";
    print {$request} "commit\n" or die "cannot commit tracking update\n";
    my $line = <$reply>;
    defined($line) && $line eq "commit: ok\n" or die "tracking transaction was not committed\n";
    1;
};
my $error = $@;
# EOF aborts a started transaction that has not committed, releasing its locks.
close $request;
close $reply;
waitpid($pid, 0);
warn $error unless $ok;
exit($ok && $? == 0 ? 0 : 1);
PERL
    then
        echo 'release delivered; tracking refresh failed or raced; fetch to refresh Git status' >&2
    fi
}
select_release
if [[ "$delivery" == pr ]]; then
    # shellcheck source=scripts/ci/release-pr.sh
    source "$runner_dir/release-pr.sh"
    pr_select
    printf 'Release %s -> %s via PR into %s at %s: prepare, review, validate merged commit, tag\n' "$previous" "$candidate" "$branch" "$destination"
else
    printf 'Release %s -> %s on %s via %s: validate, prepare, stage, commit/tag, atomic push\n' "$previous" "$candidate" "$branch" "$remote"
fi
while [[ "$phase" != complete ]]; do
    case "$phase" in
        preflight)
            assert_source
            hook release-preflight || fail 'preflight refused; this attempt has not started validation or version preparation'
            [[ "$delivery" != pr ]] || pr_preflight
            local_tags="$(git tag --list "${tag_prefix}$candidate")"
            [[ -z "$local_tags" ]] || fail 'candidate tag already exists'
            remote_tags="$(git ls-remote --refs -- "$destination" "refs/tags/${tag_prefix}$candidate")"
            [[ -z "$remote_tags" ]] || fail 'remote candidate tag already exists'
            phase=validate
            ;;
        validate)
            assert_source
            before="$(git diff --binary HEAD | git hash-object --stdin)"
            hook release-verify
            assert_source
            after="$(git diff --binary HEAD | git hash-object --stdin)"
            [[ "$before" == "$after" ]] || fail 'source or metadata changed during validation'
            assert_destination
            # Persist exact intent before the first possible release mutation.
            # Failed validation needs no recovery plan and can start afresh.
            save_phase prepare
            ;;
        prepare)
            assert_source
            [[ "$delivery" != pr ]] || pr_prepare_branch
            current="$("$make_bin" --no-print-directory -s release-version "RELEASE_GROUP=$release_group")"
            if [[ "$current" == "$previous" ]]; then
                hook release-preflight
                hook release-prepare-version
            elif [[ "$current" != "$candidate" ]]; then
                fail 'prepared version differs from the exact saved release'
            fi
            hook release-prepared-check
            [[ "$("$make_bin" --no-print-directory -s release-version "RELEASE_GROUP=$release_group")" == "$candidate" ]] || fail 'preparation did not produce the candidate'
            save_phase stage
            ;;
        stage)
            assert_source
            hook release-prepared-check
            [[ ! -L "$plan.files" ]] || fail 'release file set is symlinked'
            hook -s release-files > "$plan.files"
            if [[ -n "$release_group" ]]; then
                group_files="$(mktemp "$plan.group-files.XXXXXX")"
                bash "$runner_dir/cargo-release-group.sh" files "$release_group" > "$group_files"
                cmp -- "$plan.files" "$group_files" || fail 'adapter release files differ from the selected group'
                rm -- "$group_files"
            fi
            release_files=()
            file_count=0
            while true; do
                path=""
                if ! IFS= read -r -d '' path; then
                    [[ -z "$path" ]] || fail 'release file output is not NUL terminated'
                    break
                fi
                [[ -n "$path" && "$path" != /* ]] || fail 'release file path must be relative'
                case "/$path/" in *'/../'*|*'/./'*|*'//'*) fail 'release file path is not canonical' ;; esac
                release_files[file_count]="$path"
                file_count=$((file_count + 1))
            done < "$plan.files"
            [[ "$file_count" -gt 0 ]] || fail 'release file set is empty'
            git add -- "${release_files[@]}"
            index_tree="$(git write-tree)"
            save_phase commit
            ;;
        commit)
            if [[ "$(git rev-parse HEAD)" == "$source" ]]; then
                hook release-commit-check
                [[ "$(git write-tree)" == "$index_tree" ]] || fail 'release index changed after staging'
                git commit -m "$release_label $candidate"
            fi
            assert_release_commit
            hook release-committed-check
            if [[ "$delivery" == pr ]]; then
                prepared_commit="$release_commit"
                pr_store
                save_phase pr-publish
            else
                save_phase tag
            fi
            ;;
        pr-publish) pr_publish ;;
        pr-review) pr_review ;;
        pr-validate) pr_validate ;;
        pr-tag) pr_tag ;;
        pr-push) pr_push ;;
        tag)
            assert_release_commit
            tags="$(git tag --list "${tag_prefix}$candidate")"
            if [[ -z "$tags" ]]; then
                git tag -a "${tag_prefix}$candidate" "$release_commit" -m "$release_label $candidate"
            fi
            assert_release_tag
            hook release-tagged-check
            save_phase push
            ;;
        push)
            assert_release_commit
            assert_release_tag
            local_tag="$(git rev-parse "refs/tags/${tag_prefix}$candidate")"
            hook release-push-check
            assert_release_commit
            assert_release_tag
            [[ "$(git rev-parse "refs/tags/${tag_prefix}$candidate")" == "$local_tag" ]] || fail 'release tag changed during its final check'
            assert_destination
            local_head="$release_commit"
            read_remote_refs
            # A confirmed descendant already includes this release. Preserve that
            # branch tip when publishing a missing tag; never rewind it.
            if [[ -n "$remote_head" && "$remote_head" != "$local_head" && "$(git rev-parse HEAD)" != "$release_commit" ]]; then
                git merge-base --is-ancestor "$remote_head" HEAD || fail 'remote branch is unavailable locally or diverged; fetch and reconcile before retrying'
                if git merge-base --is-ancestor "$release_commit" "$remote_head"; then local_head="$remote_head"; fi
            fi
            if [[ "$remote_head" != "$local_head" || "$remote_tag" != "$local_tag" ]]; then
                [[ -z "$remote_tag" ]] || fail 'remote tag conflicts with the saved atomic push'
                push_source=HEAD
                if [[ "$(git rev-parse HEAD)" != "$local_head" ]]; then push_source="$local_head"; fi
                assert_destination
                # Dispatch to the captured URL even if remote configuration
                # changes after the final check. Never re-resolve its name.
                git push --no-follow-tags --atomic -- "$destination" \
                    "$push_source:refs/heads/$branch" "refs/tags/${tag_prefix}$candidate:refs/tags/${tag_prefix}$candidate"
                read_remote_refs
                [[ "$remote_head" == "$local_head" && "$remote_tag" == "$local_tag" ]] || fail 'remote push identity could not be verified; retain the plan and reconcile'
            fi
            save_phase complete
            ;;
        *) fail 'release plan phase is invalid' ;;
    esac
done
if [[ "$delivery" == pr ]]; then
    pr_assert_merged
    assert_release_tag
    [[ "$remote_tag" == "$(git rev-parse "refs/tags/${tag_prefix}$candidate")" ]] || fail 'completed PR release tag no longer matches its destination'
else
    assert_release_commit
    assert_release_tag
    assert_destination
    read_remote_refs
    [[ "$remote_tag" == "$(git rev-parse "refs/tags/${tag_prefix}$candidate")" ]] || fail 'completed release tag no longer matches its destination'
    if [[ -z "$remote_head" ]] || ! git merge-base --is-ancestor "$release_commit" "$remote_head"; then
        fail 'completed release is absent from the observed branch history; fetch and reconcile'
    fi
    refresh_release_tracking
fi
printf 'Release %s completed; plan: %s\n' "$candidate" "$plan"
