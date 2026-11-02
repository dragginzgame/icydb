#!/usr/bin/env bash
# Source-only admission precedes setup, validation, publication and mutation.
set -euo pipefail
release_root="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
export PATH="$release_root/.tools/host/bin:$PATH"
[[ -n "${RELEASE_GROUP:-}" ]] || { echo 'Select RELEASE_GROUP=database or RELEASE_GROUP=cli' >&2; exit 2; }
release_selection="$(jq -er -L "$release_root/scripts/ci" --arg group "$RELEASE_GROUP" '
    include "cargo-release-groups"; release_policy |
    [.groups[] | select(.name == $group)] |
    if length == 1 then .[0] | [.primary,.changelog] | @tsv
    else error("unknown RELEASE_GROUP") end' "$release_root/ci/cargo-release-groups.json")" || exit 2
IFS=$'\t' read -r RELEASE_PRIMARY RELEASE_CHANGELOG <<< "$release_selection"
export RELEASE_GROUP RELEASE_PRIMARY RELEASE_CHANGELOG
