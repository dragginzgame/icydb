#!/usr/bin/env bash
set -euo pipefail
source scripts/release/group.sh
export PATH="$PWD/.tools/host/bin:$PATH"
version="$(bash scripts/release/version.sh)" || exit 1
[[ "$version" == "${RELEASE_VERSION:?}" ]] || exit 1
for path in "$RELEASE_CHANGELOG" "releases/$RELEASE_GROUP/${version%.*}.md"; do
    awk -v heading="## [$version] - ${RELEASE_DATE:?}" \
        '$0 == heading { n++ } END { if (n != 1) exit 1 }' "$path"
done
bash vendor/toko-tooling/scripts/ci/cargo-release-group.sh check "$RELEASE_GROUP"
YQ="${YQ:-$PWD/.tools/host/bin/yq}" bash vendor/toko-tooling/scripts/ci/check-dependency-pins.sh --cargo-inheritance
