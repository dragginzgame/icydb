#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
[[ $# == 3 ]] || { echo 'usage: check-pocketic-binary.sh VERSION SHA256 EXECUTABLE' >&2; exit 2; }
version="$1" digest="$2" executable="$3"
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo 'PocketIC version must be exact stable SemVer' >&2; exit 2;
}
[[ -f "$executable" && -x "$executable" ]] || { echo 'PocketIC must be an executable file' >&2; exit 1; }
# Read-only file symlinks are allowed: authenticate the selected bytes before
# executing. The caller owns the reviewed host-specific identity and selection.
bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$digest" "$executable"
[[ "$executable" == /* ]] || executable="$PWD/$executable"
actual="$("$executable" --version)" || { echo 'PocketIC version probe failed' >&2; exit 1; }
[[ "$actual" == "pocket-ic-server $version" ]] || { echo 'PocketIC version mismatch' >&2; exit 1; }
