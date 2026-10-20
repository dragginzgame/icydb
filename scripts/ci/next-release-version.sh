#!/usr/bin/env bash
# Shared companions: scripts/ci/validate-release-inputs.pl
set -euo pipefail

[[ $# == 2 ]] || { echo 'usage: next-release-version.sh X.Y.Z patch|minor|major' >&2; exit 2; }
# Keep one arithmetic owner and the public command's invalid-input status.
perl "$(dirname "${BASH_SOURCE[0]}")/validate-release-inputs.pl" next-version "$1" "$2" || exit 2
