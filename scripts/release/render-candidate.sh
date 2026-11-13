#!/usr/bin/env bash
# Exact group projection; IC Build owns Cargo syntax and selected edits.
set -euo pipefail
editor="${CARGO_RELEASE_METADATA_EDITOR:?select the prepared IC Build editor}"
limit="${CARGO_RELEASE_METADATA_MAX_BYTES:?select a positive byte allowance}"
[[ "$editor" == /* && -f "$editor" && -x "$editor" && "$limit" =~ ^[1-9][0-9]*$ ]] || exit 1
[[ "${1:-}" != --check ]] || exit 0
[[ $# == 5 ]] || { echo 'usage: render-candidate.sh PATH SOURCE PREVIOUS NEXT PLAN_JSON' >&2; exit 2; }
path="$1" source_file="$2" previous="$3" next="$4" plan="$5"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
jq -e --arg path "$path" '.files | index($path) != null' "$plan" >/dev/null
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-group-projection.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
case "$path" in
    Cargo.toml|*/Cargo.toml)
        jq -e --arg path "$path" '.manifests[] | select(.path == $path) | {version,dependencies}' "$plan" > "$scratch/edit.json"
        "$editor" --max-input-bytes "$limit" group-manifest "$source_file" "$previous" "$next" "$scratch/edit.json"
        ;;
    Cargo.lock|qualification/Cargo.lock)
        packages=()
        if [[ "$path" == qualification/Cargo.lock ]]; then
            jq -r '.qualification.members[]' "$plan" > "$scratch/members"
        else
            jq -er '.members[]' "$plan" > "$scratch/members"
        fi
        while IFS= read -r package; do packages+=("$package"); done < "$scratch/members"
        if [[ "${#packages[@]}" == 0 ]]; then
            cat "$source_file"
        else
            "$editor" --max-input-bytes "$limit" lock "$source_file" "$previous" "$next" "${packages[@]}"
        fi
        ;;
    ci/dependency-pinning-exceptions.json)
        packages_json="$(jq -ce '[.dependencies[].alias]' "$plan")"
        jq --arg previous "$previous" --arg release "$next" --argjson packages "$packages_json" \
            -f "$root/scripts/release/pin-exceptions.jq" "$source_file"
        ;;
    releases/*/*.md)
        awk -v version="$next" -v previous="$previous" -v date="${RELEASE_DATE:?}" \
            -f "$root/vendor/toko-tooling/scripts/ci/finalize-release-changelog.awk" "$source_file"
        ;;
    *) echo "Unsupported release projection: $path" >&2; exit 1 ;;
esac
