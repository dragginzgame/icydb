#!/usr/bin/env bash
set -euo pipefail
# Finalize the numbered candidate in its already selected minor-line file.
# Published notes stay in their original files; development owns pending moves.
candidate_line="${RELEASE_VERSION:?}"
candidate_line="${candidate_line%.*}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-notes.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
candidate="docs/changelog/$candidate_line.md"
if [[ -f "$candidate" ]]; then
    cp -p "$candidate" "$scratch/candidate"
else
    printf '# %s\n\n' "$candidate_line" > "$scratch/candidate"
fi
awk -v version="$RELEASE_VERSION" -v date="${RELEASE_DATE:?}" \
    -f scripts/ci/finalize-release-changelog.awk "$scratch/candidate" > "$scratch/finalized"
awk -v version="$RELEASE_VERSION" -v date="$RELEASE_DATE" \
    -f scripts/ci/finalize-release-changelog.awk CHANGELOG.md > "$scratch/root-finalized"
# Replace only after every selection/finalization check succeeded.
cp "$scratch/finalized" "$candidate"
cp "$scratch/root-finalized" CHANGELOG.md
