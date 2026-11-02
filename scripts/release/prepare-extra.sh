#!/usr/bin/env bash
# The enclosing consumer transaction restores these shared constraints on failure.
set -euo pipefail
source scripts/release/group.sh
scratch="$(mktemp "${TMPDIR:-/tmp}/icydb-group-pins.XXXXXX")"
trap 'rm -f "$scratch"' EXIT
packages="$(jq -ce --arg group "$RELEASE_GROUP" '.groups[] | select(.name == $group) | .dependencies' ci/cargo-release-groups.json)"
jq --arg previous "${RELEASE_PREVIOUS:?}" --arg release "${RELEASE_VERSION:?}" --argjson packages "$packages" \
    -f scripts/release/pin-exceptions.jq ci/dependency-pinning-exceptions.json > "$scratch"
cat "$scratch" > ci/dependency-pinning-exceptions.json
