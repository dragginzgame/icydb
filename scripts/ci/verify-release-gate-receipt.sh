#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXPECTED_COMMIT="${1:-}"
publication=false
if [[ "${2:-}" == --publication && $# == 2 ]]; then publication=true
elif [[ $# != 1 ]]; then exit 2; fi

if [[ ! "$EXPECTED_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Usage: $0 <full-commit-sha>" >&2
    exit 2
fi

source "$ROOT_DIR/scripts/release/group.sh"
RELEASE_RECEIPT_DIR="$(release_receipt_directory)" || exit
shopt -s nullglob
for receipt in "$RELEASE_RECEIPT_DIR"/"$RELEASE_GROUP"-v*.commit; do
    [[ ! -L "$receipt" && -f "$receipt" ]] || continue
    [[ "$(wc -c < "$receipt")" -le 1024 ]] || continue
    receipt_commit="$(sed -n '1p' "$receipt")"
    if [[ "$receipt_commit" == "format=selected-$RELEASE_GROUP/1" ]]; then
        # Selected source validation permits the exact metadata transition and
        # push. Publication must still perform normal final-version Cargo archive
        # verification, rather than treating this as an old full-gate receipt.
        [[ "$publication" == false &&
           "$(wc -l < "$receipt")" -eq 3 ]] || continue
        receipt_commit="$(sed -n '2p' "$receipt")"
        [[ "$receipt_commit" == "$EXPECTED_COMMIT" ]] || continue
        candidate="${receipt%.commit}.candidate"
        [[ -f "$candidate" && ! -L "$candidate" ]] || continue
        [[ "$(bash "$ROOT_DIR/vendor/toko-tooling/scripts/ci/verify-file-checksum.sh" --print sha256 "$candidate")" == "$(sed -n '3p' "$receipt")" ]] || continue
        RELEASE_COMMIT="$EXPECTED_COMMIT" bash "$ROOT_DIR/scripts/ci/release-candidate-receipt.sh" verify-commit || continue
    fi
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
