#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT_DIR/.cache/release-receipts}"

workspace_version="$(
    awk '
        /^\[workspace.package\]/ { in_section = 1; next }
        /^\[/ && in_section { exit }
        in_section && $1 == "version" {
            gsub(/"/, "", $3);
            print $3;
            exit;
        }
    ' "$ROOT_DIR/Cargo.toml"
)"

if [ -z "$workspace_version" ]; then
    echo "Failed to determine workspace version from Cargo.toml" >&2
    exit 1
fi

release_tag="v$workspace_version"
head_commit="$(git -C "$ROOT_DIR" rev-parse --verify HEAD)"
tag_type="$(git -C "$ROOT_DIR" cat-file -t "refs/tags/$release_tag" 2>/dev/null || true)"
tag_commit="$(git -C "$ROOT_DIR" rev-parse --verify "refs/tags/$release_tag^{commit}" 2>/dev/null || true)"

if [ "$tag_type" != "tag" ] || [ "$tag_commit" != "$head_commit" ]; then
    echo "Release receipt requires annotated tag $release_tag at HEAD" >&2
    exit 1
fi

mkdir -p "$RELEASE_RECEIPT_DIR"
receipt="$RELEASE_RECEIPT_DIR/$release_tag.commit"
temporary_receipt="$receipt.tmp.$$"
printf '%s\n' "$head_commit" > "$temporary_receipt"
mv "$temporary_receipt" "$receipt"

echo "Recorded release-gate receipt for $release_tag at $head_commit"
