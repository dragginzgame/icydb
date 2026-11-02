#!/usr/bin/env bash
set -euo pipefail
source scripts/release/group.sh
bash scripts/ci/cargo-release-group.sh files "$RELEASE_GROUP"
