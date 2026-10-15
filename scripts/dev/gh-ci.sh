#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: gh-ci.sh [--branch <branch>] [--commit <revision>] [--workflow <name> | --all-workflows] [--run <id>] [--failed] [--logs] [--list] [--limit <n>]

Inspect GitHub Actions CI through an authenticated local GitHub CLI session.

Options:
  --branch <branch>   Branch or tag name to inspect. Defaults to the current git branch.
                      Detached HEAD requires --branch, --commit or --run.
  --workflow <name>   Workflow name to inspect. Defaults to CI.
  --commit <revision> Resolve a local revision (e.g. HEAD) to an exact commit.
                      No implicit branch filter when this option is selected.
  --all-workflows     List runs across workflows, bounded by --limit.
  --run <id>          Inspect a specific workflow run id.
                      Cannot combine with list, branch, workflow or failure filters.
  --failed            Search historical failures, even if a later run passed.
  --logs              Print failed-step logs for the selected attempt; if empty,
                      retrieve only its failed jobs' logs. Retain recovery evidence.
  --list              List recent runs instead of opening one run; excludes --logs.
  --limit <n>         Number of runs to list. Defaults to 10.
  -h, --help          Show this help.

Examples:
  gh-ci.sh
  gh-ci.sh --failed --logs
  gh-ci.sh --branch main --list
  gh-ci.sh --commit HEAD --all-workflows --limit 100
USAGE
}

require_command() {
    local command_name="$1"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "missing required tool: $command_name" >&2
        exit 1
    fi
}

require_gh_auth() {
    if ! gh auth status >/dev/null 2>&1; then
        echo "gh is not authenticated; run: gh auth login -h github.com --git-protocol https --web" >&2
        exit 1
    fi
}

current_branch() {
    git branch --show-current
}

# Retain both streams before interpreting an observation. In particular, a
# paginated API failure must not dispatch downloads from its partial job list.
observe() {
    local name="$1" status=0
    shift
    "$@" > "$logs/$name" 2> "$logs/$name.errors" || status=$?
    cat "$logs/$name.errors" >&2
    if [[ "$status" != 0 ]]; then
        cat "$logs/$name"
        return "$status"
    fi
}

workflow="CI"
workflow_selected=0
all_workflows=0
commit=""
branch=""
run_id=""
status_filter=""
print_logs=0
list_runs=0
limit=10

while [ "$#" -gt 0 ]; do
    case "$1" in
        --branch)
            if [[ "$#" -lt 2 || -z "$2" ]]; then
                usage
                exit 2
            fi
            branch="$2"
            shift 2
            ;;
        --workflow)
            if [[ "$#" -lt 2 || -z "$2" ]]; then
                usage
                exit 2
            fi
            workflow="$2"
            workflow_selected=1
            shift 2
            ;;
        --commit)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            commit="$2"
            shift 2
            ;;
        --all-workflows)
            all_workflows=1
            list_runs=1
            shift
            ;;
        --run)
            if [[ "$#" -lt 2 || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
                echo '--run must be a positive integer' >&2
                exit 2
            fi
            run_id="$2"
            shift 2
            ;;
        --failed)
            status_filter="failure"
            shift
            ;;
        --logs)
            print_logs=1
            shift
            ;;
        --list)
            list_runs=1
            shift
            ;;
        --limit)
            if [ "$#" -lt 2 ]; then
                usage
                exit 2
            fi
            limit="$2"
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

[[ "$limit" =~ ^[1-9][0-9]*$ ]] || { echo '--limit must be a positive integer' >&2; exit 2; }
if [[ "$all_workflows" == 1 && ( "$workflow_selected" == 1 || -n "$run_id" || "$print_logs" == 1 ) ]]; then
    echo '--all-workflows lists runs; use --run <id> --logs to inspect a listed run' >&2
    exit 2
fi
if [[ -n "$commit" && -n "$run_id" ]]; then
    echo '--commit selects runs; it cannot be combined with --run' >&2
    exit 2
fi
if [[ -n "$run_id" && ( "$list_runs" == 1 || -n "$branch" || "$workflow_selected" == 1 || -n "$status_filter" ) ]]; then
    echo '--run selects one run; it cannot be combined with list or search filters' >&2
    exit 2
fi
if [[ "$list_runs" == 1 && "$print_logs" == 1 ]]; then
    echo '--list cannot be combined with --logs; select a run to inspect its logs' >&2
    exit 2
fi

require_command gh
require_command git

if [[ -n "$commit" ]]; then
    commit="$(git rev-parse --verify --end-of-options "$commit^{commit}")" || {
        echo 'cannot resolve the selected local commit' >&2; exit 1;
    }
    [[ "$commit" =~ ^[0-9a-f]{40}$ || "$commit" =~ ^[0-9a-f]{64}$ ]] || {
        echo 'git returned an invalid commit identity' >&2; exit 1;
    }
    printf 'Selected commit: %s\n' "$commit" >&2
elif [[ -z "$run_id" && -z "$branch" ]]; then
    branch="$(current_branch)" || {
        status=$?
        echo 'cannot observe the current Git branch; select --branch, --commit or --run' >&2
        exit "$status"
    }
    [[ -n "$branch" ]] || {
        echo 'no current Git branch (detached HEAD); select --branch, --commit or --run' >&2
        exit 1
    }
fi
require_gh_auth

list_args=(run list)
if [[ "$all_workflows" == 0 ]]; then list_args+=(--workflow "$workflow"); fi
if [[ -n "$branch" ]]; then list_args+=(--branch "$branch"); fi
if [[ -n "$commit" ]]; then list_args+=(--commit "$commit"); fi
if [[ -n "$status_filter" ]]; then
    list_args+=(--status "$status_filter")
    echo 'Historical failure search; later successful runs are excluded.' >&2
fi

if [ "$list_runs" -eq 1 ]; then
    if [[ "$all_workflows" == 1 ]]; then
        printf 'Listing at most %s runs across workflows; this is not a complete CI verdict.\n' "$limit" >&2
    fi
    gh "${list_args[@]}" --limit "$limit"
    exit 0
fi

if [ -z "$run_id" ]; then
    run_id="$(gh "${list_args[@]}" --limit 1 --json databaseId --jq '.[0].databaseId // ""')"
fi

if [ -z "$run_id" ]; then
    if [ -n "$commit" ]; then
        echo "no matching $workflow runs found for commit $commit" >&2
    elif [ -n "$branch" ]; then
        echo "no matching $workflow runs found for $branch" >&2
    else
        echo "no matching $workflow runs found" >&2
    fi
    exit 1
fi

[[ "$run_id" =~ ^[1-9][0-9]*$ ]] || { echo 'invalid run identity' >&2; exit 1; }

if [ "$print_logs" -eq 1 ]; then
    logs="$(mktemp -d "${TMPDIR:-/tmp}/gh-ci-logs.XXXXXX")"
    retain_logs=0
    trap 'status=$?; if [[ "$status" == 0 && "$retain_logs" == 0 ]]; then rm -rf "$logs"; else printf "\nCI log observation retained: %s\n" "$logs" >&2; fi' EXIT
    # `gh repo view` does not honour GH_REPO on every supported CLI version.
    repo_args=()
    if [[ -n "${GH_REPO:-}" ]]; then repo_args+=("$GH_REPO"); fi
    observe repository.txt gh repo view ${repo_args[@]+"${repo_args[@]}"} --json url --jq .url
    repository="$(cat "$logs/repository.txt")"
    [[ "$repository" =~ ^https://([A-Za-z0-9.-]+)/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)$ ]] || {
        echo 'invalid repository identity' >&2; exit 1;
    }
    api_host="${BASH_REMATCH[1]}"
    api_repo="${BASH_REMATCH[2]}/${BASH_REMATCH[3]}"
    repository="$api_host/$api_repo"
    observe run.tsv gh api --hostname "$api_host" "repos/$api_repo/actions/runs/$run_id" \
        --jq '[.id,.run_attempt,.status,.conclusion] | @tsv'
    IFS=$'\t' read -r observed_run attempt state < "$logs/run.tsv"
    [[ "$observed_run" == "$run_id" && "$attempt" =~ ^[1-9][0-9]*$ ]] || {
        echo 'invalid run or attempt identity' >&2; exit 1;
    }
    # Pin both CLI and API reads before any logs are requested. A newly started
    # rerun must not redirect this inspection to another attempt.
    gh run view "$run_id" --verbose --repo "$repository" --attempt "$attempt"
    status=0
    gh run view "$run_id" --log-failed --repo "$repository" --attempt "$attempt" \
        > "$logs/failed.log" 2> "$logs/errors.log" || status=$?
    cat "$logs/failed.log"
    cat "$logs/errors.log" >&2
    [[ "$status" == 0 ]] || exit "$status"
    # A zero CLI status with empty output does not establish that failure logs
    # were inspected. Only a completed non-failing run can lack them normally.
    if ! LC_ALL=C grep -q '[^[:space:]]' "$logs/failed.log"; then
        case "$state" in
            $'completed\tsuccess'|$'completed\tneutral'|$'completed\tskipped')
                printf 'No failed-step logs for completed %s run %s.\n' "${state#*$'\t'}" "$run_id" >&2 ;;
            $'completed\tfailure'|$'completed\ttimed_out')
                retain_logs=1
                observe jobs.tsv gh api --hostname "$api_host" \
                    "repos/$api_repo/actions/runs/$run_id/attempts/$attempt/jobs?per_page=100" \
                    --paginate --jq '.jobs[] | select(.conclusion == "failure" or .conclusion == "timed_out") | [.run_id,.run_attempt,.id] | @tsv'
                [[ -s "$logs/jobs.tsv" ]] || {
                    echo 'failed-step logs unavailable: no failed jobs in the selected attempt' >&2; exit 1;
                }
                # Admit the complete list before requesting any job's log.
                while IFS=$'\t' read -r job_run job_attempt job_id; do
                    [[ "$job_run" == "$run_id" && "$job_attempt" == "$attempt" && "$job_id" =~ ^[1-9][0-9]*$ ]] || {
                        echo 'failed job does not match the selected run and attempt' >&2; exit 1;
                    }
                done < "$logs/jobs.tsv"
                while IFS=$'\t' read -r job_run job_attempt job_id; do
                    printf '\nFull failed-job log: %s run %s attempt %s job %s\n' "$repository" "$run_id" "$attempt" "$job_id"
                    observe "job-$job_id.log" gh api --hostname "$api_host" "repos/$api_repo/actions/jobs/$job_id/logs"
                    cat "$logs/job-$job_id.log"
                    LC_ALL=C grep -q '[^[:space:]]' "$logs/job-$job_id.log" || {
                        printf 'failed-job logs unavailable for job %s\n' "$job_id" >&2; exit 1;
                    }
                done < "$logs/jobs.tsv"
                ;;
            *)
                printf 'failed-step logs unavailable for run %s; inspect its job details before concluding the failure review.\n' "$run_id" >&2
                exit 1 ;;
        esac
    fi
else
    gh run view "$run_id" --verbose
fi
