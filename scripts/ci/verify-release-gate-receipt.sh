#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT_DIR/.cache/release-receipts}"
EXPECTED_COMMIT="${1:-}"

if [[ ! "$EXPECTED_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Usage: $0 <full-commit-sha>" >&2
    exit 2
fi

shopt -s nullglob
for receipt in "$RELEASE_RECEIPT_DIR"/v*.commit; do
    receipt_commit="$(sed -n '1p' "$receipt")"
    if [[ "$receipt_commit" != "$EXPECTED_COMMIT" ]]; then
        continue
    fi

    release_tag="$(basename "$receipt" .commit)"
    tag_type="$(git -C "$ROOT_DIR" cat-file -t "refs/tags/$release_tag" 2>/dev/null || true)"
    tag_commit="$(
        git -C "$ROOT_DIR" rev-parse --verify "refs/tags/$release_tag^{commit}" 2>/dev/null || true
    )"
    if [[ "$tag_type" == "tag" && "$tag_commit" == "$EXPECTED_COMMIT" ]]; then
        exit 0
    fi
done

exit 1
