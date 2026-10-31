#!/usr/bin/env bash
# Shared companions: scripts/ci/check-make-execution.sh scripts/ci/cargo-publication-selection.pl scripts/ci/cargo-publication-paths.sh scripts/ci/run-cargo-publication.sh docs/cargo-publication.md
set -euo pipefail
[[ $# == 1 && ( "$1" == check || "$1" == publish ) ]] || { echo 'select check or publish' >&2; exit 2; }
mode="$1"
self="${BASH_SOURCE[0]%/*}"
bash "$self/check-make-execution.sh"
# shellcheck source=scripts/ci/cargo-publication-paths.sh
source "$self/cargo-publication-paths.sh"
all_features="${CARGO_PUBLISH_ALL_FEATURES:-0}"
no_defaults="${CARGO_PUBLISH_NO_DEFAULT_FEATURES:-0}"
allow_dirty="${CARGO_PUBLISH_CHECK_ALLOW_DIRTY:-0}"
for value in "$all_features" "$no_defaults" "$allow_dirty"; do
    [[ "$value" == 0 || "$value" == 1 ]] || { echo 'publication options require 0 or 1' >&2; exit 2; }
done
features="${CARGO_PUBLISH_FEATURES:-}"
[[ "$features" != -* && "$features" != *$'\r'* && "$features" != *$'\n'* ]] || exit 2
[[ "$all_features" != 1 || -z "$features" ]] || { echo 'select all features or named features' >&2; exit 2; }
options=() metadata_options=()
manifest="${CARGO_PUBLISH_MANIFEST_PATH:-}" target_dir="${CARGO_PUBLISH_TARGET_DIR:-}"
if [[ -n "$manifest" || -n "$target_dir" ]]; then
    checkout="$(git rev-parse --show-toplevel && printf '/.')"
    checkout="${checkout%$'\n/.'}"
    case "$checkout" in *$'\r'*|*$'\n'*) exit 2 ;; esac
    checkout="$(CDPATH='' cd -P -- "$checkout" && printf '%s/.' "$PWD")"
    checkout="${checkout%/.}"
    if [[ -n "$manifest" ]]; then
        publication_path_option --manifest-path "$manifest" "$checkout"
        metadata_options=(--manifest-path "$manifest")
        options+=(--manifest-path "$manifest")
    fi
    if [[ -n "$target_dir" ]]; then
        publication_path_option --target-dir "$target_dir" "$checkout"
        options+=(--target-dir "$target_dir")
    fi
fi
[[ "$all_features" != 1 ]] || options+=(--all-features)
[[ "$no_defaults" != 1 ]] || options+=(--no-default-features)
[[ -z "$features" ]] || options+=(--features "$features")
[[ "$mode" != check || "$allow_dirty" != 1 ]] || options+=(--allow-dirty)
registry="${CARGO_PUBLISH_REGISTRY:-}"
[[ "$registry" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || { echo 'declare a named publication registry' >&2; exit 2; }
verify_text="${CARGO_PUBLISH_VERIFY_TARGETS:-}"
[[ -n "$verify_text" && "$verify_text" != *$'\n'* && "$verify_text" != *$'\r'* ]] || exit 2
IFS=$' \t' read -r -a verify_targets <<< "$verify_text"
[[ ${#verify_targets[@]} -gt 0 ]] || exit 2
for target in "${verify_targets[@]}"; do
    case "$target" in ''|-*|*=*|publish|publish-check) echo 'publication validation requires non-publication Make targets' >&2; exit 2 ;; esac
done
parent="${CARGO_PUBLISH_EVIDENCE_ROOT:?declare an ignored evidence parent}"
case "$parent" in *$'\n'*|*$'\r'*) exit 2 ;; esac
mkdir -p "$parent"
parent="$(cd -P "$parent" && printf '%s/.' "$PWD")"
parent="${parent%/.}"
selection="$(mktemp -d "$parent/selection.XXXXXX")"
printf 'Publication selection evidence: %s\n' "$selection"
source="$(git rev-parse --verify HEAD)"
printf '%s\n' "$source" > "$selection/source"
status=0
cargo metadata --no-deps --locked --offline --format-version 1 ${metadata_options[@]+"${metadata_options[@]}"} \
    > "$selection/metadata.json" 2> "$selection/metadata.stderr" || status=$?
cat "$selection/metadata.stderr" >&2 || :
[[ "$status" == 0 ]] || exit "$status"
perl "$self/cargo-publication-selection.pl" "$selection/metadata.json" \
    "${CARGO_PUBLISH_PACKAGES:-}" "${CARGO_PUBLISH_SELECTION:-}" "$registry" > "$selection/packages.nul"
arguments=()
while IFS= read -r -d '' package; do arguments+=(--package "$package"); done < "$selection/packages.nul"
[[ ${#arguments[@]} -gt 0 ]] || exit 2
for target in "${verify_targets[@]}"; do
    "${CARGO_PUBLISH_MAKE:-make}" --no-print-directory "$target"
done
# Every invocation uses the same parent lock; unique selection evidence is never
# the runner's lock namespace. Retained incomplete attempts prevent blind replay.
attempt="$parent/$mode.${selection##*/}"
exec bash "$self/run-cargo-publication.sh" "$mode" "$attempt" "$source" "$registry" "${arguments[@]}" ${options[@]+"${options[@]}"}
