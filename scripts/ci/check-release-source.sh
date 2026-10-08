#!/usr/bin/env bash
set -euo pipefail

# Run at the checkout root. Consumers own the exact permitted metadata paths
# and the release phase; this read-only helper neither stages nor repairs them.
usage() { echo 'usage: check-release-source.sh [--allow RELATIVE-PATH]...' >&2; }
allowed=()
while [[ $# -gt 0 ]]; do
    [[ "$1" == --allow && $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
    case "/$2/" in
        //*|*/../*|*/./*|*//*) usage; exit 2 ;;
    esac
    allowed+=("$2")
    shift 2
done
export GIT_OPTIONAL_LOCKS=0
prefix="$(git rev-parse --show-prefix)" || { echo 'cannot inspect release checkout' >&2; exit 1; }
[[ -z "$prefix" ]] || { echo 'release source check requires the checkout root' >&2; exit 1; }
scratch="$(mktemp -d "${TMPDIR:-/tmp}/release-source.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
# Porcelain keeps index/worktree columns separate and computes actual changes
# even with stale stat data. Optional locks are disabled to preserve index bytes.
git status --porcelain=v1 -z --untracked-files=all > "$scratch/status" || {
    echo 'cannot inspect release-source status' >&2; exit 1;
}
rejected=false
admit_path() {
    local path="$1" state="$2" selected
    for selected in ${allowed[@]+"${allowed[@]}"}; do
        [[ "$path" != "$selected" ]] || return 0
    done
    if [[ "$rejected" == false ]]; then
        echo 'release source refused: uncommitted paths outside the allowed metadata set' >&2
    fi
    printf '  %s: %q\n' "$state" "$path" >&2
    rejected=true
}
while IFS= read -r -d '' entry; do
    status="${entry:0:2}" path="${entry:3}"
    if [[ "$status" == '??' ]]; then
        admit_path "$path" untracked
    else
        [[ "${status:0:1}" == ' ' ]] || admit_path "$path" staged
        [[ "${status:1:1}" == ' ' ]] || admit_path "$path" unstaged
        # In -z output, a rename/copy's original path follows its destination.
        if [[ "$status" == *R* || "$status" == *C* ]]; then
            IFS= read -r -d '' original || { echo 'incomplete Git rename observation' >&2; exit 1; }
            [[ "${status:0:1}" != R && "${status:0:1}" != C ]] || admit_path "$original" staged
            [[ "${status:1:1}" != R && "${status:1:1}" != C ]] || admit_path "$original" unstaged
        fi
    fi
done < "$scratch/status"
[[ "$rejected" == false ]]
