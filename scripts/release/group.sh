#!/usr/bin/env bash
# Source-only admission precedes setup, validation, publication and mutation.
set -euo pipefail
release_root="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
export PATH="$release_root/.tools/host/bin:$PATH"
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
    [[ $# == 0 || ( $# == 1 && $1 == --batch ) ]] || exit 2
fi
[[ -n "${RELEASE_GROUP:-}" ]] || { echo 'Select RELEASE_GROUP=database or RELEASE_GROUP=cli' >&2; exit 2; }
if [[ ${BASH_SOURCE[0]} == "$0" && ${1:-} == --batch ]]; then
    # Group membership has one policy owner. Check local dependency edges too:
    # a batch must never publish a consumer before a selected supplier.
    jq -e -L "$release_root/scripts/ci" --arg groups "${RELEASE_GROUPS:-}" --arg first "$RELEASE_GROUP" '
        include "cargo-release-groups"; release_policy |
        (.groups | map(.name)) as $known | ($groups | split(",")) as $selected |
        $selected[0] == $first and ($selected | length > 0) and
        ($selected | length) == ($selected | unique | length) and
        all($selected[]; . as $name | $known | index($name) != null)
    ' "$release_root/ci/cargo-release-groups.json" >/dev/null || exit 2
    if [[ $RELEASE_GROUPS == *,* ]]; then
        metadata=$(cd "$release_root" && cargo metadata --locked --offline --no-deps --format-version 1) || exit
        jq -e --arg groups "$RELEASE_GROUPS" --slurpfile policy "$release_root/ci/cargo-release-groups.json" '
            ($groups | split(",")) as $selected |
            (reduce $policy[0].groups[] as $g ({};
                reduce $g.members[] as $name (. ; .[$name] = $g.name))) as $owner |
            all(.packages[]; . as $p |
                all(.dependencies[] | select(.path != null); . as $d |
                    ($selected | index($owner[$p.name])) as $consumer |
                    ($selected | index($owner[$d.name])) as $supplier |
                    $consumer == null or $supplier == null or $supplier <= $consumer))
        ' <<< "$metadata" >/dev/null || {
            echo 'Select release groups in supplier-first order' >&2; exit 2;
        }
    fi
    exit 0
fi
release_selection="$(jq -er -L "$release_root/scripts/ci" --arg group "$RELEASE_GROUP" '
    include "cargo-release-groups"; release_policy |
    [.groups[] | select(.name == $group)] |
    if length == 1 then .[0] | [.primary,.changelog] | @tsv
    else error("unknown RELEASE_GROUP") end' "$release_root/ci/cargo-release-groups.json")" || exit 2
IFS=$'\t' read -r RELEASE_PRIMARY RELEASE_CHANGELOG <<< "$release_selection"
export RELEASE_GROUP RELEASE_PRIMARY RELEASE_CHANGELOG

# Release qualification belongs beside the Git recovery journal, not in Cargo
# or executable caches. Explicit fixture/operator selections retain their owner.
release_receipt_directory() {
    if [[ -n "${RELEASE_RECEIPT_DIR:-}" ]]; then
        printf '%s\n' "$RELEASE_RECEIPT_DIR"
    else
        local git_directory
        git_directory="$(git -C "$release_root" rev-parse --absolute-git-dir)" || return
        printf '%s/release-state/receipts\n' "$git_directory"
    fi
}
