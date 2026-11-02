#!/usr/bin/env bash
set -euo pipefail
root="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
source "$root/scripts/release/group.sh"
cd "$root"
bash scripts/ci/cargo-release-group.sh version "$RELEASE_GROUP"
