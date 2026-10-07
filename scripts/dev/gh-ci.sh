#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: gh-ci.sh [--branch <branch>] [--commit <revision>] [--workflow <name> | --all-workflows] [--run <id>] [--failed] [--logs] [--list] [--limit <n>]

Inspect GitHub Actions CI through an authenticated local GitHub CLI session.

Options:
  --branch <branch>   Branch or tag name to inspect. Defaults to the current git branch.
  --workflow <name>   Workflow name to inspect. Defaults to CI.
  --commit <revision> Resolve a local revision (e.g. HEAD) to an exact commit.
                      No implicit branch filter when this option is selected.
  --all-workflows     List runs across workflows, bounded by --limit.
  --run <id>          Inspect a specific workflow run id.
  --failed            Search historical failures, even if a later run passed.
  --logs              Print failed-step logs after the run summary.
  --list              List recent runs instead of opening one run.
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
    git branch --show-current 2>/dev/null || true
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
            if [ "$#" -lt 2 ]; then
                usage
                exit 2
            fi
            branch="$2"
            shift 2
            ;;
        --workflow)
            if [ "$#" -lt 2 ]; then
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
            if [ "$#" -lt 2 ]; then
                usage
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
elif [ -z "$branch" ]; then
    branch="$(current_branch)"
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

gh run view "$run_id" --verbose

if [ "$print_logs" -eq 1 ]; then
    gh run view "$run_id" --log-failed
fi
