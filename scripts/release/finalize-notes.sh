#!/usr/bin/env bash
set -euo pipefail
source scripts/release/group.sh
# Finalize the numbered candidate in its already selected minor-line file.
# Published notes stay in their original files; development owns pending moves.
candidate_line="${RELEASE_VERSION:?}"
candidate_line="${candidate_line%.*}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-notes.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
candidate="releases/$RELEASE_GROUP/$candidate_line.md"
jq -e --arg group "$RELEASE_GROUP" --arg path "$candidate" ' .groups[] | select(.name == $group) | .metadata_files | index($path) != null' ci/cargo-release-groups.json >/dev/null
cp -p "$candidate" "$scratch/candidate"
awk -v version="$RELEASE_VERSION" -v date="${RELEASE_DATE:?}" \
    -f scripts/ci/finalize-release-changelog.awk "$scratch/candidate" > "$scratch/finalized"
# Shared preparation already finalized the group ledger. Admit that identity
# before publishing the independently rendered detail notes.
awk -v heading="## [$RELEASE_VERSION] - $RELEASE_DATE" \
    '$0 == heading {n++} END {if(n != 1)exit 1}' "$RELEASE_CHANGELOG"
cp "$scratch/finalized" "$candidate"
