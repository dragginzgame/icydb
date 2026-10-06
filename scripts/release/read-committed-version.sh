#!/usr/bin/env bash
set -euo pipefail

# IcyDB owns the exact Git source; the shared reader owns TOML/SemVer admission.
[[ $# == 2 && "$2" =~ ^[0-9a-f]{40}$ ]] || exit 2
ROOT="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$ROOT/.tools/host/bin:$PATH"
snapshot="$(mktemp -d "${TMPDIR:-/tmp}/icydb-committed-version.XXXXXX")" || exit 1
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then rm -rf "$snapshot"
    else echo "Committed version inputs retained: $snapshot" >&2; fi
    exit "$status"
}
trap finish EXIT
# Cargo validates root-package targets too. Export the selected committed tree,
# never combine its manifest with newer working sources or targets.
git -C "$ROOT" archive --format=tar "$2" > "$snapshot/source.tar" || exit 1
mkdir "$snapshot/tree" || exit 1
tar -xf "$snapshot/source.tar" -C "$snapshot/tree" || exit 1
bash "$SCRIPT_DIR/../ci/read-cargo-workspace-version.sh" --stable "$snapshot/tree/Cargo.toml"
