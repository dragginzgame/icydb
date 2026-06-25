#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: scripts/dev/delete-github-tags-up-to.sh [options]

Deletes semver tags up to and including a cutoff. The default cutoff is v0.160.0.

Options:
  --cutoff VERSION     cutoff version/tag, with or without a leading v
  --remote NAME        git remote to delete tags from (default: origin)
  --delete-local       delete matching local tags
  --delete-remote      delete matching remote tags
  --yes                required with delete flags
  -h, --help           show this help

Examples:
  scripts/dev/delete-github-tags-up-to.sh
  scripts/dev/delete-github-tags-up-to.sh --delete-local --yes
  scripts/dev/delete-github-tags-up-to.sh --delete-local --delete-remote --yes
USAGE
}

parse_version() {
    local version="$1"

    if [[ "${version}" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        printf "%d %d %d\n" \
            "$((10#${BASH_REMATCH[1]}))" \
            "$((10#${BASH_REMATCH[2]}))" \
            "$((10#${BASH_REMATCH[3]}))"
        return
    fi

    echo "error: invalid semver tag '${version}'" >&2
    exit 2
}

version_le() {
    local lhs="$1"
    local rhs="$2"
    local lhs_major lhs_minor lhs_patch
    local rhs_major rhs_minor rhs_patch

    read -r lhs_major lhs_minor lhs_patch < <(parse_version "${lhs}")
    read -r rhs_major rhs_minor rhs_patch < <(parse_version "${rhs}")

    ((lhs_major < rhs_major)) && return 0
    ((lhs_major > rhs_major)) && return 1
    ((lhs_minor < rhs_minor)) && return 0
    ((lhs_minor > rhs_minor)) && return 1
    ((lhs_patch <= rhs_patch))
}

select_tags() {
    local cutoff="$1"
    local tag

    while IFS= read -r tag; do
        if [[ "${tag}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] && version_le "${tag}" "${cutoff}"; then
            printf "%s\n" "${tag}"
        fi
    done | sort -V
}

local_tags() {
    git tag --list '*.*.*' | select_tags "${cutoff}"
}

remote_tags() {
    git ls-remote --tags --refs "${remote}" \
        | awk '{ sub("^refs/tags/", "", $2); print $2 }' \
        | select_tags "${cutoff}"
}

print_tags() {
    local label="$1"
    shift
    local -a tags=("$@")

    printf "%s tags selected: %d\n" "${label}" "${#tags[@]}"
    if ((${#tags[@]} > 0)); then
        printf "%s\n" "${tags[@]}"
    fi
}

delete_local_tags() {
    local -a batch=()
    local tag

    for tag in "$@"; do
        batch+=("${tag}")
        if ((${#batch[@]} == 50)); then
            git tag -d "${batch[@]}"
            batch=()
        fi
    done

    if ((${#batch[@]} > 0)); then
        git tag -d "${batch[@]}"
    fi
}

delete_remote_tags() {
    local -a batch=()
    local tag

    for tag in "$@"; do
        batch+=(":refs/tags/${tag}")
        if ((${#batch[@]} == 50)); then
            git push "${remote}" "${batch[@]}"
            batch=()
        fi
    done

    if ((${#batch[@]} > 0)); then
        git push "${remote}" "${batch[@]}"
    fi
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "${script_dir}" rev-parse --show-toplevel)"
cd "${repo_root}"

cutoff="v0.160.0"
remote="origin"
delete_local=false
delete_remote=false
confirmed=false

while (($# > 0)); do
    case "$1" in
        --cutoff)
            if (($# < 2)); then
                echo "error: --cutoff requires a value" >&2
                exit 2
            fi
            cutoff="$2"
            shift 2
            ;;
        --remote)
            if (($# < 2)); then
                echo "error: --remote requires a value" >&2
                exit 2
            fi
            remote="$2"
            shift 2
            ;;
        --delete-local)
            delete_local=true
            shift
            ;;
        --delete-remote)
            delete_remote=true
            shift
            ;;
        --yes)
            confirmed=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option '$1'" >&2
            usage
            exit 2
            ;;
    esac
done

parse_version "${cutoff}" >/dev/null

if [[ "${delete_remote}" == true ]]; then
    git remote get-url "${remote}" >/dev/null
fi

readarray -t selected_local_tags < <(local_tags)
print_tags "local" "${selected_local_tags[@]}"

if [[ "${delete_remote}" == true ]]; then
    readarray -t selected_remote_tags < <(remote_tags)
    print_tags "remote ${remote}" "${selected_remote_tags[@]}"
fi

if [[ "${delete_local}" == false && "${delete_remote}" == false ]]; then
    echo "dry run only; pass --delete-local and/or --delete-remote with --yes to delete tags"
    exit 0
fi

if [[ "${confirmed}" != true ]]; then
    echo "refusing to delete tags without --yes" >&2
    exit 2
fi

if [[ "${delete_local}" == true && ${#selected_local_tags[@]} -gt 0 ]]; then
    delete_local_tags "${selected_local_tags[@]}"
fi

if [[ "${delete_remote}" == true && ${#selected_remote_tags[@]} -gt 0 ]]; then
    delete_remote_tags "${selected_remote_tags[@]}"
fi
