#!/usr/bin/env bash
# Pure consumer projections; IC Build owns Cargo syntax and selected edits.
set -euo pipefail
editor="${CARGO_RELEASE_METADATA_EDITOR:?prepare the released IC Build metadata editor and select its absolute path}"
limit="${CARGO_RELEASE_METADATA_MAX_BYTES:?select a positive per-document byte allowance}"
[[ "$editor" == /* && -f "$editor" && -x "$editor" && "$limit" =~ ^[1-9][0-9]*$ ]] || { echo 'CARGO_RELEASE_METADATA_EDITOR must name an absolute prepared executable' >&2; exit 1; }
if [[ $# == 1 && "$1" == --check ]]; then exit 0; fi
[[ $# == 5 ]] || { echo 'usage: render-candidate.sh PATH SOURCE PREVIOUS NEXT METADATA_JSON' >&2; exit 2; }
path="$1" source_file="$2" previous="$3" next="$4" metadata="$5"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)" || exit 1
case "$path" in
    Cargo.toml)
        scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-manifest-projection.XXXXXX")" || exit 1
        complete=false
        finish_projection() {
            local status=$?
            [[ "$complete" == true || "$status" != 0 ]] || status=1
            if [[ "$status" == 0 ]]; then rm -rf "$scratch"
            else printf 'Failed projection retained: %s\n' "$scratch" >&2; fi
            exit "$status"
        }
        trap finish_projection EXIT
        "${YQ:-yq}" -p toml -o json '.' "$source_file" > "$scratch/manifest.json" || exit 1
        jq -n --arg root "${ICYDB_RELEASE_ROOT:-$root}" --slurpfile manifest "$scratch/manifest.json" --slurpfile metadata "$metadata" '
            {source_root: $root, manifest: $manifest[0], members: ($metadata[0] | . as $m |
                [.packages[] | select(.id as $id | $m.workspace_members | index($id)) |
                    {name, manifest_path}])}' > "$scratch/context.json" || exit 1
        "$editor" --max-input-bytes "$limit" manifest "$source_file" "$previous" "$next" "$scratch/context.json" || exit 1
        complete=true
        ;;
    Cargo.lock)
        packages=()
        # Capture admission before iteration: process substitution must not hide
        # a failed metadata observation from the parent command.
        names="$(jq -er '. as $m | .packages[] | select(.id as $id | $m.workspace_members | index($id)) | .name' "$metadata")" || exit 1
        while IFS= read -r package; do packages[${#packages[@]}]="$package"; done <<< "$names"
        "$editor" --max-input-bytes "$limit" lock "$source_file" "$previous" "$next" "${packages[@]}"
        ;;
    ci/dependency-pinning-exceptions.json)
        packages_json="$(jq '[. as $m | .packages[] | select(.id as $id | $m.workspace_members | index($id)) | .name]' "$metadata")" || exit 1
        jq --arg previous "$previous" --arg release "$next" --argjson packages "$packages_json" \
            -f "$root/scripts/release/pin-exceptions.jq" "$source_file"
        ;;
    *) cat "$source_file" ;;
esac
