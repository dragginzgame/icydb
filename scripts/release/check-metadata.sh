#!/usr/bin/env bash
set -euo pipefail
version="$(cargo get workspace.package.version)"
[[ "$version" == "${RELEASE_VERSION:?}" ]]
for path in CHANGELOG.md "docs/changelog/${version%.*}.md"; do
    awk -v heading="## [$version] - ${RELEASE_DATE:?}" \
        '$0 == heading { n++ } END { if (n != 1) exit 1 }' "$path"
done
cargo metadata --locked --offline --no-deps --format-version 1 >/dev/null
