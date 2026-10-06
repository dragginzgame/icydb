#!/usr/bin/env bash
set -euo pipefail
export PATH="$PWD/.tools/host/bin:$PATH"
version="$(bash scripts/ci/read-cargo-workspace-version.sh --stable "$PWD/Cargo.toml")" || exit 1
[[ "$version" == "${RELEASE_VERSION:?}" ]] || exit 1
for path in CHANGELOG.md "docs/changelog/${version%.*}.md"; do
    awk -v heading="## [$version] - ${RELEASE_DATE:?}" \
        '$0 == heading { n++ } END { if (n != 1) exit 1 }' "$path"
done
cargo metadata --locked --offline --no-deps --format-version 1 >/dev/null
YQ="${YQ:-$PWD/.tools/host/bin/yq}" bash scripts/ci/check-dependency-pins.sh --cargo-inheritance
