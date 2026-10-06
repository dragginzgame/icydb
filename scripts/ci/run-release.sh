#!/usr/bin/env bash
set -euo pipefail

# Canonical ordering and Git effects; consumer targets own metadata and gates.
# Dependencies: Bash 3.2, GNU Make, Git, date and standard Unix file utilities.
# Journals live in the repository's Git directory, outside build artifacts.
usage() {
    echo 'usage: run-release.sh patch|minor|major REMOTE BRANCH' >&2
    echo '       run-release.sh resume X.Y.Z REMOTE BRANCH' >&2
    exit 2
}
fail() { echo "release refused: $1" >&2; exit 1; }
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
git check-ref-format "refs/heads/$branch" >/dev/null
root="$(git rev-parse --show-toplevel)"
cd "$root"
[[ "$(git symbolic-ref --quiet --short HEAD)" == "$branch" ]] || fail 'selected branch is not checked out'
destination="$(git remote get-url --push --all "$remote")"
[[ -n "$destination" && "$destination" != *$'\n'* ]] || fail 'release requires exactly one push URL'
remote_identity="$(printf '%s\n' "$destination" | git hash-object --stdin)"
state_root="$(git rev-parse --git-path release-state)"
[[ ! -L "$state_root" ]] || fail 'release state directory is symlinked'
mkdir -p "$state_root"
mkdir "$state_root/lock" 2>/dev/null || fail 'release lock is occupied; inspect its owner before clearing a stale lock'
trap 'rmdir "$state_root/lock"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "$$" > "$state_root/lock/owner"
trap 'rm -f "$state_root/lock/owner"; rmdir "$state_root/lock"' EXIT

make_bin="${RELEASE_MAKE:-make}"
hook() {
    "$make_bin" --no-print-directory "$@" \
        "RELEASE_KIND=$kind" "RELEASE_PREVIOUS=$previous" \
        "RELEASE_VERSION=$candidate" "RELEASE_DATE=$release_date" \
        "RELEASE_REMOTE=$remote" "RELEASE_BRANCH=$branch" "RELEASE_SOURCE=$source" \
        "RELEASE_COMMIT=${release_commit:-}"
}
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
        extra=""
        if IFS= read -r extra || [[ -n "$extra" ]]; then fail 'release plan has extra records'; fi
    } < "$1"
    [[ "$saved_schema" == release-plan-1 && "$saved_candidate" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ && "$1" == "$state_root/$saved_candidate.plan" ]] || fail 'release plan identity is invalid'
    [[ "$saved_source" =~ ^[0-9a-f]{40,64}$ && "$saved_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && "$saved_destination" =~ ^[0-9a-f]{40,64}$ ]] || fail 'release plan source/date/destination is invalid'
    [[ "$(bash "$(dirname "${BASH_SOURCE[0]}")/next-release-version.sh" "$saved_previous" "$saved_kind")" == "$saved_candidate" ]] || fail 'release plan increment is invalid'
    case "$saved_phase" in preflight|validate|prepare|stage|commit|tag|push|complete) ;; *) fail 'release plan phase is invalid' ;; esac
}
load_plan() {
    read_plan "$plan"
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
    plan=""
    release_commit=""
    followup=no
    if [[ "$mode" == resume ]]; then
        [[ "$requested_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || usage
        plan="$state_root/$requested_version.plan"
    else
        # Select saved intent before reading possibly bumped metadata. Recovery uses
        # the saved increment, not another increment from the prepared version.
        early_intent=no
        for retained_plan in "$state_root"/*.plan; do
            [[ -e "$retained_plan" || -L "$retained_plan" ]] || continue
            read_plan "$retained_plan"
            case "$saved_phase" in
                complete) ;;
                preflight|validate) early_intent=yes ;;
                *)
                    [[ -z "$plan" ]] || fail 'multiple unfinished release identities; inspect retained plans'
                    plan="$retained_plan"
                    ;;
            esac
        done
        [[ -z "$plan" || "$early_intent" == no ]] || fail 'multiple unfinished release identities; inspect retained plans'
    fi
    if [[ -n "$plan" ]]; then
        load_plan
        if [[ "$mode" != resume && "$kind" != "$mode" ]]; then
            case "$phase" in
                commit|tag|push)
                    [[ "$(git rev-parse HEAD)" != "$source" ]] || fail "unfinished release $candidate is not yet committed; rerun make release-$kind to finish its preparation"
                    followup=yes
                    ;;
                *) fail "unfinished release $candidate is not yet committed; rerun make release-$kind to finish its preparation" ;;
            esac
        fi
    else
        kind="$mode"
        previous="$("$make_bin" --no-print-directory -s release-version)"
        candidate="$(bash "$(dirname "${BASH_SOURCE[0]}")/next-release-version.sh" "$previous" "$kind")"
        release_date="$(date -u +%F)"
        source="$(git rev-parse --verify HEAD)"
        plan="$state_root/$candidate.plan"
        # Validation has no release mutations to resume. Retain its old identity as
        # evidence, then validate the current source afresh through the normal target.
        # Once preparation starts, even a failed bump may have effects to reconcile.
        for retained_plan in "$state_root"/*.plan; do
            [[ -e "$retained_plan" || -L "$retained_plan" ]] || continue
            read_plan "$retained_plan"
            case "$saved_phase" in
                complete)
                    [[ "$saved_candidate" != "$candidate" ]] || fail "release $candidate is already complete; inspect its metadata and tag"
                    ;;
                preflight|validate)
                    [[ "$saved_previous" == "$previous" && -z "$saved_tree" && ! -e "$retained_plan.files" && ! -L "$retained_plan.files" ]] || fail "release $saved_candidate has possible preparation effects; reconcile its saved plan"
                    [[ "$remote" == "$saved_remote" && "$branch" == "$saved_branch" && "$remote_identity" == "$saved_destination" ]] || fail 'release destination changed'
                    retained_tags="$(git tag --list "v$saved_candidate")"
                    [[ -z "$retained_tags" ]] || fail 'preparation-free retry conflicts with a local release tag'
                    retained_tags="$(git ls-remote --refs -- "$destination" "refs/tags/v$saved_candidate")"
                    [[ -z "$retained_tags" ]] || fail 'preparation-free retry conflicts with a remote release tag'
                    archive="$(mktemp -d "$state_root/$saved_candidate.attempt.XXXXXX")"
                    mv "$retained_plan" "$archive/$saved_candidate.plan"
                    printf 'Restarting before preparation; retained earlier attempt: %s\n' "$archive/$saved_candidate.plan"
                    ;;
                *) fail 'release plan changed during selection' ;;
            esac
        done
        index_tree=""
        phase=preflight
    fi
}

save_phase() {
    phase="$1"
    temporary_plan="$(mktemp "$plan.tmp.XXXXXX")"
    printf '%s\n' release-plan-1 "$kind" "$previous" "$candidate" "$release_date" \
        "$source" "$remote" "$branch" "$remote_identity" "$index_tree" "$phase" > "$temporary_plan"
    mv "$temporary_plan" "$plan"
}
assert_source() { [[ "$(git rev-parse HEAD)" == "$source" ]] || fail 'validated source commit changed'; }
assert_release_commit() {
    # The first direct descendant must be the exact staged release. Later fixes
    # may be committed without changing its identity or repeating its commit.
    if [[ -z "$release_commit" ]]; then
        history="$(git rev-list --first-parent --reverse "$source..HEAD")"
        release_commit="${history%%$'\n'*}"
    fi
    [[ "$release_commit" =~ ^[0-9a-f]{40,64}$ && "$(git log -1 --format=%P "$release_commit")" == "$source" && "$(git log -1 --format=%s "$release_commit")" == "Release $candidate" ]] || fail 'release commit does not match the saved source/version'
    [[ "$index_tree" =~ ^[0-9a-f]{40,64}$ && "$(git rev-parse "$release_commit^{tree}")" == "$index_tree" ]] || fail 'release commit tree differs from the exact staged release'
    git merge-base --is-ancestor "$release_commit" HEAD || fail 'release commit is not an ancestor of the selected branch'
    [[ "$("$make_bin" --no-print-directory -s release-version)" == "$candidate" ]] || fail 'release metadata no longer matches the candidate'
    git diff --quiet HEAD -- || fail 'release worktree differs from the committed payload'
    untracked="$(git ls-files --others --exclude-standard)" || fail 'cannot inventory untracked release source'
    [[ -z "$untracked" ]] || fail 'release worktree has untracked source'
    if [[ "$mode" != resume && "$(git rev-parse HEAD)" != "$release_commit" ]]; then followup=yes; fi
}
assert_release_tag() {
    [[ "$(git cat-file -t "refs/tags/v$candidate")" == tag && "$(git rev-parse "refs/tags/v$candidate^{commit}")" == "$release_commit" ]] || fail 'existing tag differs from the exact release commit'
}
read_remote_refs() {
    refs="$(git ls-remote --refs -- "$destination" "refs/heads/$branch" "refs/tags/v$candidate")"
    remote_head=""
    remote_tag=""
    while read -r sha ref extra; do
        [[ -n "$sha" || -n "$ref" ]] || continue
        [[ "$sha" =~ ^[0-9a-f]{40,64}$ && -z "$extra" ]] || fail 'remote ref output is malformed'
        case "$ref" in
            "refs/heads/$branch") [[ -z "$remote_head" ]] || fail 'remote branch is duplicated'; remote_head="$sha" ;;
            "refs/tags/v$candidate") [[ -z "$remote_tag" ]] || fail 'remote tag is duplicated'; remote_tag="$sha" ;;
            *) fail 'remote ref output contains an unexpected identity' ;;
        esac
    done <<< "$refs"
}
while true; do
    select_release
    printf 'Release %s -> %s on %s via %s: validate, prepare, stage, commit/tag, atomic push\n' "$previous" "$candidate" "$branch" "$remote"
    while [[ "$phase" != complete ]]; do
        case "$phase" in
            preflight)
                assert_source
                hook release-preflight
                local_tags="$(git tag --list "v$candidate")"
                [[ -z "$local_tags" ]] || fail 'candidate tag already exists'
                remote_tags="$(git ls-remote --refs -- "$destination" "refs/tags/v$candidate")"
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
                # Persist exact intent before the first possible release mutation.
                # Failed validation needs no recovery plan and can start afresh.
                save_phase prepare
                ;;
            prepare)
                assert_source
                current="$("$make_bin" --no-print-directory -s release-version)"
                if [[ "$current" == "$previous" ]]; then
                    hook release-preflight
                    hook release-prepare-version
                elif [[ "$current" != "$candidate" ]]; then
                    fail 'prepared version differs from the exact saved release'
                fi
                hook release-prepared-check
                [[ "$("$make_bin" --no-print-directory -s release-version)" == "$candidate" ]] || fail 'preparation did not produce the candidate'
                save_phase stage
                ;;
            stage)
                assert_source
                hook release-prepared-check
                [[ ! -L "$plan.files" ]] || fail 'release file set is symlinked'
                hook -s release-files > "$plan.files"
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
                    git commit -m "Release $candidate"
                fi
                assert_release_commit
                hook release-committed-check
                save_phase tag
                ;;
            tag)
                assert_release_commit
                tags="$(git tag --list "v$candidate")"
                if [[ -z "$tags" ]]; then
                    git tag -a "v$candidate" "$release_commit" -m "Release $candidate"
                fi
                assert_release_tag
                hook release-tagged-check
                save_phase push
                ;;
            push)
                assert_release_commit
                assert_release_tag
                hook release-push-check
                local_head="$release_commit"
                local_tag="$(git rev-parse "refs/tags/v$candidate")"
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
                    git push --no-follow-tags --atomic "$remote" \
                        "$push_source:refs/heads/$branch" "refs/tags/v$candidate:refs/tags/v$candidate"
                    read_remote_refs
                    [[ "$remote_head" == "$local_head" && "$remote_tag" == "$local_tag" ]] || fail 'remote push identity could not be verified; retain the plan and reconcile'
                fi
                save_phase complete
                ;;
            *) fail 'release plan phase is invalid' ;;
        esac
    done
    assert_release_commit
    printf 'Release %s completed; retained plan: %s\n' "$candidate" "$plan"
    [[ "$followup" == yes ]] || break
    printf 'Saved release reconciled; validating current source for make release-%s.\n' "$mode"
done
