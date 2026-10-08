#!/usr/bin/env bash
# Shared companions: scripts/ci/install-ci-tool.sh
set -euo pipefail

SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
exec bash "$SCRIPT_DIR/install-ci-tool.sh" actionlint "$@"
