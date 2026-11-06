#!/usr/bin/env bash
set -euo pipefail
root="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
source "$root/scripts/release/group.sh"
cd "$root"
# Frozen delivery queries its version before setup. Cargo's manifest-only
# observation needs neither downloaded dependencies nor the prepared TOML tool;
# complete graph/group admission still runs before validation and mutation.
metadata="$(cargo metadata --locked --offline --no-deps --format-version 1)" || exit
jq -er --arg primary "$RELEASE_PRIMARY" '
    . as $workspace |
    [.packages[] | select(.name==$primary and
        (.id as $id | $workspace.workspace_members | index($id)))] |
    if length==1 then .[0].version else error("group primary is not unique") end
' <<< "$metadata"
