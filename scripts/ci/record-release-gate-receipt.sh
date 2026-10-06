#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT_DIR/.cache/release-receipts}"

release_commit="${RELEASE_COMMIT:-$(git -C "$ROOT_DIR" rev-parse --verify HEAD)}"
[[ "$release_commit" =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid selected release commit" >&2; exit 1; }
git -C "$ROOT_DIR" merge-base --is-ancestor "$release_commit" HEAD || {
    echo "Selected release commit is not on the current history" >&2
    exit 1
}
workspace_version="$(bash "$ROOT_DIR/scripts/release/read-committed-version.sh" "$ROOT_DIR" "$release_commit")" || exit 1

release_tag="v$workspace_version"
tag_type="$(git -C "$ROOT_DIR" cat-file -t "refs/tags/$release_tag" 2>/dev/null || true)"
tag_commit="$(git -C "$ROOT_DIR" rev-parse --verify "refs/tags/$release_tag^{commit}" 2>/dev/null || true)"

if [ "$tag_type" != "tag" ] || [ "$tag_commit" != "$release_commit" ]; then
    echo "Release receipt requires annotated tag $release_tag at the selected commit" >&2
    exit 1
fi

mkdir -p "$RELEASE_RECEIPT_DIR"
receipt="$RELEASE_RECEIPT_DIR/$release_tag.commit"
temporary_receipt="$receipt.tmp.$$"
printf '%s\n' "$release_commit" > "$temporary_receipt"
mv "$temporary_receipt" "$receipt"

echo "Recorded release-gate receipt for $release_tag at $release_commit"
