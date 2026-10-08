#!/usr/bin/env bash
# Sourced by run-release.sh for explicit PR delivery. The runner owns the lock,
# preparation and phase journal; this file owns GitHub PR identity and transport.
# Never source a consumer checkout's copy while qualifying a merged commit.
# shellcheck disable=SC2154 # Shared state is supplied by run-release.sh.

pr_select() {
    command -v gh >/dev/null
    command -v jq >/dev/null
    case "$destination" in
        https://github.com/*) pr_repository="${destination#https://github.com/}" ;;
        git@github.com:*) pr_repository="${destination#git@github.com:}" ;;
        ssh://git@github.com/*) pr_repository="${destination#ssh://git@github.com/}" ;;
        *) fail 'PR delivery requires an explicit github.com repository push URL' ;;
    esac
    pr_repository="${pr_repository%.git}"
    [[ "$pr_repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail 'invalid GitHub release repository'
    pr_branch="release/v$candidate"
    [[ "$pr_branch" != "$branch" ]] || fail 'release PR head and base must differ'
    pr_state="$plan.pr.json"
    pr_number=""
    pr_attempted=false
    merged_commit=""
    if [[ -e "$pr_state" || -L "$pr_state" ]]; then
        [[ -f "$pr_state" && ! -L "$pr_state" ]] || fail 'PR identity file is not regular'
        jq -e --arg repository "$pr_repository" '
          type == "object" and keys == ["attempted","merged","number","prepared","repository"] and
          .repository == $repository and (.attempted | type == "boolean") and
          (.number == "" or (.number | test("^[1-9][0-9]*$"))) and
          all(.prepared,.merged; type == "string" and (. == "" or test("^[0-9a-f]{40,64}$")))
        ' "$pr_state" >/dev/null || fail 'PR identity file is invalid'
        prepared_commit="$(jq -r .prepared "$pr_state")"
        merged_commit="$(jq -r .merged "$pr_state")"
        pr_number="$(jq -r .number "$pr_state")"
        pr_attempted="$(jq -r .attempted "$pr_state")"
    else
        case "$phase" in preflight|validate|prepare) ;; *) fail 'saved PR identity is missing' ;; esac
    fi
    case "$phase" in
        preflight|validate)
            [[ "$(git symbolic-ref --quiet --short HEAD)" == "$branch" ]] || fail 'selected PR base branch is not checked out'
            ;;
        stage|commit)
            [[ "$(git symbolic-ref --quiet --short HEAD)" == "$pr_branch" ]] || fail 'prepared release branch is not checked out'
            ;;
        prepare|pr-publish|pr-review)
            local selected
            selected="$(git symbolic-ref --quiet --short HEAD)"
            [[ "$selected" == "$branch" || "$selected" == "$pr_branch" ]] || fail 'release checkout is on another branch'
            ;;
    esac
}

pr_store() {
    local temporary
    [[ ! -L "$pr_state" ]] || fail 'PR identity file is symlinked'
    temporary="$(mktemp "$pr_state.tmp.XXXXXX")"
    jq -n --arg repository "$pr_repository" --arg prepared "$prepared_commit" \
        --arg merged "$merged_commit" --arg number "$pr_number" --argjson attempted "$pr_attempted" \
        '{repository:$repository,prepared:$prepared,merged:$merged,number:$number,attempted:$attempted}' > "$temporary"
    mv "$temporary" "$pr_state"
}

pr_preflight() {
    read_remote_refs
    [[ "$remote_head" == "$source" ]] || fail 'PR preparation requires the current published base commit'
    local head_ref
    head_ref="$(git ls-remote --refs -- "$destination" "refs/heads/$pr_branch")"
    [[ -z "$head_ref" ]] || fail 'release PR branch already exists before preparation'
    # Read-only API admission also establishes that this token can see this repo.
    gh api --hostname github.com "repos/$pr_repository" --jq .full_name > "$state_root/pr-repository"
    [[ "$(cat "$state_root/pr-repository")" == "$pr_repository" ]] || fail 'GitHub repository identity differs from the push destination'
}

pr_prepare_branch() {
    [[ -e "$pr_state" ]] || pr_store
    if git show-ref --verify --quiet "refs/heads/$pr_branch"; then
        [[ "$(git rev-parse "refs/heads/$pr_branch")" == "$source" ]] || fail 'local release branch differs from the validated source'
    else
        git branch "$pr_branch" "$source"
    fi
    git switch "$pr_branch"
}

pr_assert_prepared() {
    [[ "$prepared_commit" =~ ^[0-9a-f]{40,64}$ &&
       "$(git log -1 --format=%P "$prepared_commit")" == "$source" &&
       "$(git log -1 --format=%s "$prepared_commit")" == "Release $candidate" &&
       "$(git rev-parse "$prepared_commit^{tree}")" == "$index_tree" ]] || fail 'prepared PR commit identity changed'
    [[ "$(git rev-parse "refs/heads/$pr_branch")" == "$prepared_commit" ]] || fail 'local PR branch changed'
    git diff --cached --quiet HEAD -- || fail 'release index contains unrelated changes'
    git diff --quiet -- || fail 'release working files changed'
    [[ -z "$(git ls-files --others --exclude-standard)" ]] || fail 'release checkout has untracked source'
}

pr_observe_head() {
    local refs sha ref extra
    refs="$(git ls-remote --refs -- "$destination" "refs/heads/$pr_branch")"
    pr_remote_head=""
    while read -r sha ref extra; do
        [[ -n "$sha" || -n "$ref" ]] || continue
        [[ "$sha" =~ ^[0-9a-f]{40,64}$ && "$ref" == "refs/heads/$pr_branch" && -z "$extra" && -z "$pr_remote_head" ]] || fail 'remote PR branch observation is invalid'
        pr_remote_head="$sha"
    done <<< "$refs"
}

pr_query() {
    local response count observed
    response="$(mktemp "$plan.github.XXXXXX")"
    gh api --hostname github.com --method GET "repos/$pr_repository/pulls" \
        -f state=all -f "head=${pr_repository%/*}:$pr_branch" -f "base=$branch" \
        --paginate > "$response"
    # jq owns page aggregation; packaged gh versions need not support --slurp.
    # Require a successful query and at least one array page before any effect.
    jq -se 'length > 0 and all(.[]; type == "array")' "$response" >/dev/null || fail 'invalid PR list response'
    count="$(jq -s '[.[][]] | length' "$response")"
    [[ "$count" == 0 || "$count" == 1 ]] || fail 'multiple release PRs require reconciliation'
    if [[ "$count" == 0 ]]; then
        [[ -z "$pr_number" ]] || fail 'saved release PR disappeared from its selected repository/base/head'
        return
    fi
    observed="$(jq -ser '[.[][]][0].number | select(type == "number" and . > 0 and floor == .) | tostring' "$response")"
    [[ -z "$pr_number" || "$pr_number" == "$observed" ]] || fail 'release PR identity changed'
    pr_number="$observed"
    pr_details="$(mktemp "$plan.github.XXXXXX")"
    gh api --hostname github.com "repos/$pr_repository/pulls/$pr_number" > "$pr_details"
    jq -e --arg repository "$pr_repository" --arg head "$pr_branch" --arg base "$branch" \
        --arg commit "$prepared_commit" --argjson number "$pr_number" '
          .number == $number and .base.repo.full_name == $repository and .head.repo.full_name == $repository and
          .base.ref == $base and .head.ref == $head and .head.sha == $commit and
          (.merged | type == "boolean") and (.state == "open" or .state == "closed")
        ' "$pr_details" >/dev/null || fail 'release PR payload or destination changed'
    pr_store
}

pr_publish() {
    pr_assert_prepared
    assert_destination
    # A lost reply can leave an already merged PR and a deleted remote head.
    # Observe that identity before deciding whether a branch push is needed.
    pr_query
    if [[ -n "$pr_number" ]]; then save_phase pr-review; return; fi
    pr_observe_head
    if [[ -z "$pr_remote_head" ]]; then
        git push --no-follow-tags --atomic -- "$destination" "$prepared_commit:refs/heads/$pr_branch"
        pr_observe_head
    fi
    [[ "$pr_remote_head" == "$prepared_commit" ]] || fail 'remote release branch differs from the prepared commit'
    pr_query
    if [[ -z "$pr_number" ]]; then
        [[ "$pr_attempted" == false ]] || fail 'PR creation outcome is uncertain; inspect the saved request before authorizing another creation'
        local request response errors status response_status
        request="$(mktemp "$plan.request.XXXXXX")"
        response="$(mktemp "$plan.response.XXXXXX")"
        errors="$(mktemp "$plan.errors.XXXXXX")"
        jq -n --arg head "$pr_branch" --arg base "$branch" --arg title "Release $candidate" \
            --arg body "Prepared release $candidate. Merge after required review/checks, then rerun the release target to validate the merged commit and publish its tag. Publication and cleanup are separate." \
            '{head:$head,base:$base,title:$title,body:$body}' > "$request"
        pr_attempted=true
        pr_store
        status=0
        gh api --hostname github.com --method POST "repos/$pr_repository/pulls" \
            --include --input "$request" > "$response" 2> "$errors" || status=$?
        if [[ "$status" != 0 ]]; then
            # An observed rejection did not create a PR. A missing response or
            # server error remains uncertain and must be reconciled by identity.
            response_status="$(awk 'NR == 1 && /^HTTP\// { print $2 }' "$response")"
            case "$response_status" in
                401|403|404|422) pr_attempted=false; pr_store ;;
            esac
            cat "$errors" >&2
            exit "$status"
        fi
        pr_query
        [[ -n "$pr_number" ]] || fail 'created PR cannot be observed; retain its request/response and reconcile'
    fi
    save_phase pr-review
}

pr_review() {
    pr_assert_prepared
    assert_destination
    pr_query
    [[ -n "$pr_number" ]] || fail 'release PR is missing'
    if [[ "$(jq -r .merged "$pr_details")" != true ]]; then
        [[ "$(jq -r .state "$pr_details")" == open ]] || fail 'release PR was closed without merging'
        pr_observe_head
        [[ "$pr_remote_head" == "$prepared_commit" ]] || fail 'open release PR branch changed'
        printf 'Release %s awaits review/merge: https://github.com/%s/pull/%s; rerun the same target afterwards.\n' "$candidate" "$pr_repository" "$pr_number"
        exit 75
    fi
    local observed
    observed="$(jq -er '.merge_commit_sha | select(type == "string" and test("^[0-9a-f]{40,64}$"))' "$pr_details")"
    [[ -z "$merged_commit" || "$merged_commit" == "$observed" ]] || fail 'merged release commit changed'
    merged_commit="$observed"
    pr_store
    pr_assert_merged
    save_phase pr-validate
}

pr_assert_merged() {
    pr_assert_prepared
    [[ "$merged_commit" =~ ^[0-9a-f]{40,64}$ ]] || fail 'merged release identity is missing'
    assert_destination
    pr_query
    [[ "$(jq -r .merged "$pr_details")" == true && "$(jq -r .merge_commit_sha "$pr_details")" == "$merged_commit" ]] || fail 'saved PR merge identity changed'
    git fetch --no-tags --no-write-fetch-head -- "$destination" "$merged_commit"
    [[ "$(git rev-parse "$merged_commit^{tree}")" == "$index_tree" ]] || fail 'merged tree differs from the prepared release; reconcile the changed payload'
    read_remote_refs
    [[ -n "$remote_head" ]] || fail 'release base branch disappeared'
    git fetch --no-tags --no-write-fetch-head -- "$destination" "$remote_head"
    git merge-base --is-ancestor "$merged_commit" "$remote_head" || fail 'release commit is not in the selected remote base'
    # shellcheck disable=SC2034 # Consumed by the runner's tag and hook functions.
    release_commit="$merged_commit"
    # shellcheck disable=SC2034 # Consumed by the runner's hook function.
    validation_source="$merged_commit"
}

pr_worktree() {
    local state_path
    state_path="$(cd "$state_root" && pwd -P)"
    release_worktree="$state_path/$candidate.merged"
    [[ ! -L "$release_worktree" ]] || fail 'merged qualification checkout is symlinked'
    if [[ ! -e "$release_worktree" ]]; then
        git worktree add --detach "$release_worktree" "$merged_commit"
    fi
    [[ "$(git -C "$release_worktree" rev-parse --show-toplevel)" == "$release_worktree" &&
       "$(git -C "$release_worktree" rev-parse HEAD)" == "$merged_commit" ]] || fail 'merged qualification checkout changed'
    git -C "$release_worktree" diff --cached --quiet HEAD -- || fail 'merged qualification index changed'
    git -C "$release_worktree" diff --quiet -- || fail 'merged qualification source changed'
    [[ -z "$(git -C "$release_worktree" ls-files --others --exclude-standard)" ]] || fail 'merged qualification has untracked source'
}

pr_validate() {
    pr_assert_merged
    pr_worktree
    hook release-merged-preflight
    hook release-verify
    pr_worktree
    hook release-committed-check
    pr_worktree
    assert_destination
    save_phase pr-tag
}

pr_tag() {
    pr_assert_merged
    pr_worktree
    hook release-committed-check
    pr_worktree
    local tags
    tags="$(git tag --list "v$candidate")"
    if [[ -z "$tags" ]]; then git tag -a "v$candidate" "$merged_commit" -m "Release $candidate"; fi
    assert_release_tag
    hook release-tagged-check
    save_phase pr-push
}

pr_push() {
    pr_assert_merged
    pr_worktree
    assert_release_tag
    local tag
    tag="$(git rev-parse "refs/tags/v$candidate")"
    hook release-push-check
    pr_worktree
    assert_release_tag
    [[ "$(git rev-parse "refs/tags/v$candidate")" == "$tag" ]] || fail 'release tag changed during its final check'
    assert_destination
    read_remote_refs
    if [[ "$remote_tag" != "$tag" ]]; then
        [[ -z "$remote_tag" ]] || fail 'remote tag conflicts with the merged release'
        git push --no-follow-tags --atomic -- "$destination" "refs/tags/v$candidate:refs/tags/v$candidate"
        read_remote_refs
        [[ "$remote_tag" == "$tag" ]] || fail 'release tag push outcome is uncertain; retain and reconcile'
    fi
    save_phase complete
}
