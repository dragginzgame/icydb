#!/usr/bin/env bash
set -euo pipefail

if [[ ( $# != 2 && $# != 3 ) || ! "$1" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ||
    ! "$2" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    echo 'usage: check-release-tag.sh <exact-commit> <version>' >&2
    exit 2
fi
group="${3:-${RELEASE_GROUP:-}}"
[[ -z "$group" || "$group" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || exit 2
commit="$1"
version="$2"
resolved="$(git rev-parse --verify "$commit^{commit}")" || exit 1
[[ "$resolved" == "$commit" ]] || exit 1
tag="refs/tags/${group:+$group-}v$version"
kind="$(git cat-file -t "$tag")" || exit 1
[[ "$kind" == tag ]] || {
    echo "release tag v$version must be annotated" >&2; exit 1;
}
tagged_commit="$(git rev-parse --verify "$tag^{commit}")" || exit 1
[[ "$tagged_commit" == "$commit" ]] || {
    echo "release tag v$version does not point to $commit" >&2; exit 1;
}
