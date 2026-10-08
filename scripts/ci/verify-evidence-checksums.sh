#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Read retained sha256sum-format manifests; the reviewed helper owns hashing
# and GNU/Perl backend selection. This check never writes evidence or builds.
SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
if [[ "$#" == 0 ]]; then
    echo 'usage: verify-evidence-checksums.sh <manifest> [...]' >&2
    exit 2
fi
record_pattern='^([0-9a-f]{64}) [ *](.+)$'
for manifest in "$@"; do
    [[ -f "$manifest" ]] || { echo "missing checksum manifest: $manifest" >&2; exit 1; }
    count=0
    while IFS= read -r record || [[ -n "$record" ]]; do
        if [[ ! "$record" =~ $record_pattern ]]; then
            echo "malformed checksum record: $manifest:$((count + 1))" >&2
            exit 1
        fi
        digest="${BASH_REMATCH[1]}"
        file="${BASH_REMATCH[2]}"
        bash "$SCRIPT_DIR/verify-file-checksum.sh" sha256 "$digest" "$file"
        count=$((count + 1))
    done < "$manifest"
    [[ "$count" -gt 0 ]] || { echo "empty checksum manifest: $manifest" >&2; exit 1; }
    echo "evidence checksums verified: $manifest ($count files)"
done
