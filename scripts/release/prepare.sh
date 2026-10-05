#!/usr/bin/env bash
set -euo pipefail
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-metadata.XXXXXX")"
files=()
while IFS= read -r -d '' path; do files[${#files[@]}]="$path"; done < <(bash scripts/release/files.sh)
for path in "${files[@]}"; do
    [[ ! -L "$path" && ( ! -e "$path" || -f "$path" ) ]]
    mkdir -p "$scratch/$(dirname "$path")"
    if [[ -f "$path" ]]; then cp -p "$path" "$scratch/$path"; fi
done
complete=false
cleanup() {
    local status=$? path
    trap - EXIT
    if [[ "$complete" != true ]]; then
        for path in "${files[@]}"; do
            if [[ -f "$scratch/$path" ]]; then
                cp -p "$scratch/$path" "$path" || { echo "restore failed; originals: $scratch" >&2; exit 1; }
            else
                rm -f "$path" || { echo "restore failed; originals: $scratch" >&2; exit 1; }
            fi
        done
    fi
    rm -rf "$scratch"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
awk -v version="${RELEASE_VERSION:?}" -v date="${RELEASE_DATE:?}" \
    -f scripts/ci/finalize-release-changelog.awk CHANGELOG.md >/dev/null
bash scripts/ci/bump-version.sh "${RELEASE_KIND:?}"
make --no-print-directory fmt
bash scripts/release/finalize-notes.sh
bash scripts/release/check-metadata.sh
bash scripts/ci/release-candidate-receipt.sh record "$RELEASE_KIND" "${RELEASE_SOURCE:?}"
complete=true
