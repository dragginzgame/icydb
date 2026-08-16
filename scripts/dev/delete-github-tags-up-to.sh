#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'USAGE'
usage: scripts/dev/delete-github-tags-up-to.sh [options]

Deletes semver tags up to and including a cutoff. The default cutoff is the
complete v0.210 minor line, including every v0.210.x patch.

Options:
  --cutoff CUTOFF      minor or exact cutoff (default: v0.210)
                       210, 0.210, and v0.210 include every v0.210.x tag;
                       v0.210.0 is an exact semantic-version cutoff
  --remote NAME        git remote to delete tags from (default: origin)
  --delete-local       delete matching local tags
  --delete-remote      delete matching remote tags
  --yes                required with delete flags
  -h, --help           show this help

Examples:
  scripts/dev/delete-github-tags-up-to.sh
  scripts/dev/delete-github-tags-up-to.sh --cutoff v0.209
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

parse_cutoff() {
    local value="${1#v}"

    if [[ "${value}" =~ ^([0-9]+)$ ]]; then
        printf "minor 0 %d 0\n" "$((10#${BASH_REMATCH[1]}))"
        return
    fi
    if [[ "${value}" =~ ^([0-9]+)\.([0-9]+)$ ]]; then
        printf "minor %d %d 0\n" \
            "$((10#${BASH_REMATCH[1]}))" \
            "$((10#${BASH_REMATCH[2]}))"
        return
    fi
    if [[ "${value}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        printf "exact %d %d %d\n" \
            "$((10#${BASH_REMATCH[1]}))" \
            "$((10#${BASH_REMATCH[2]}))" \
            "$((10#${BASH_REMATCH[3]}))"
        return
    fi

    echo "error: invalid cutoff '${1}'" >&2
    exit 2
}

tag_is_selected() {
    local tag="$1"
    local tag_major tag_minor tag_patch

    read -r tag_major tag_minor tag_patch < <(parse_version "${tag}")

    if ((tag_major < cutoff_major)); then
        return 0
    fi
    if ((tag_major > cutoff_major)); then
        return 1
    fi
    if ((tag_minor < cutoff_minor)); then
        return 0
    fi
    if ((tag_minor > cutoff_minor)); then
        return 1
    fi
    if [[ "${cutoff_kind}" == "minor" ]]; then
        return 0
    fi
    ((tag_patch <= cutoff_patch))
}

select_tags() {
    local tag

    while IFS= read -r tag; do
        if [[ "${tag}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] && tag_is_selected "${tag}"; then
            printf "%s\n" "${tag}"
        fi
    done | sort -V
}

local_tags() {
    git tag --list '*.*.*' | select_tags
}

remote_tags() {
    git ls-remote --tags --refs "${remote}" \
        | awk '{ sub("^refs/tags/", "", $2); print $2 }' \
        | select_tags
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

cutoff="v0.210"
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

read -r cutoff_kind cutoff_major cutoff_minor cutoff_patch < <(parse_cutoff "${cutoff}")

git remote get-url "${remote}" >/dev/null

local_tag_output="$(local_tags)"
selected_local_tags=()
if [[ -n "${local_tag_output}" ]]; then
    readarray -t selected_local_tags <<<"${local_tag_output}"
fi
print_tags "local" "${selected_local_tags[@]}"

if ! remote_tag_output="$(remote_tags)"; then
    echo "error: failed to inventory tags from remote ${remote}; no tags were deleted" >&2
    exit 1
fi
selected_remote_tags=()
if [[ -n "${remote_tag_output}" ]]; then
    readarray -t selected_remote_tags <<<"${remote_tag_output}"
fi
print_tags "remote ${remote}" "${selected_remote_tags[@]}"

if [[ "${delete_local}" == false && "${delete_remote}" == false ]]; then
    echo "dry run only; pass --delete-local and/or --delete-remote with --yes to delete tags"
    exit 0
fi

if [[ "${confirmed}" != true ]]; then
    echo "refusing to delete tags without --yes" >&2
    exit 2
fi

if [[ "${delete_remote}" == true && ${#selected_remote_tags[@]} -gt 0 ]]; then
    delete_remote_tags "${selected_remote_tags[@]}"
    if ! remaining_remote_output="$(remote_tags)"; then
        echo "error: remote deletion ran, but verification of ${remote} failed; local tags were not deleted" >&2
        exit 1
    fi
    remaining_remote_tags=()
    if [[ -n "${remaining_remote_output}" ]]; then
        readarray -t remaining_remote_tags <<<"${remaining_remote_output}"
    fi
    if ((${#remaining_remote_tags[@]} > 0)); then
        echo "error: ${#remaining_remote_tags[@]} selected tags remain on remote ${remote}; local tags were not deleted" >&2
        exit 1
    fi
fi

if [[ "${delete_local}" == true && ${#selected_local_tags[@]} -gt 0 ]]; then
    if [[ "${delete_remote}" == false && ${#selected_remote_tags[@]} -gt 0 ]]; then
        echo "warning: matching tags remain on remote ${remote} and will return on the next tag fetch" >&2
    fi
    delete_local_tags "${selected_local_tags[@]}"
fi
