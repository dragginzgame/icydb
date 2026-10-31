#!/usr/bin/env bash
# Shared companions: scripts/ci/read-cargo-workspace-version.sh scripts/ci/finalize-release-changelog.awk scripts/ci/cargo-workspace-members.jq scripts/ci/compare-stable-versions.pl scripts/ci/release-metadata-transaction.sh scripts/ci/cargo-release-group.sh
set -euo pipefail

# Coordinated versions by default; explicit groups opt in. Adapters retain source admission,
# cache/tool setup, release policy, staging and publication ownership.
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
[[ $# == 2 || $# == 3 ]] || { echo 'usage: cargo-release-metadata.sh prepare|check|check-graph PRIMARY_PACKAGE [--manifest-order|--manifest-order-grouped]' >&2; exit 2; }
operation="$1" primary="$2"
check_manifest_order=false
group_manifest_order=false
if [[ $# == 3 ]]; then
    [[ "$operation" == check && ( "$3" == --manifest-order || "$3" == --manifest-order-grouped ) ]] || {
        echo 'manifest ordering is supported only by check' >&2; exit 2;
    }
    check_manifest_order=true
    [[ "$3" != --manifest-order-grouped ]] || group_manifest_order=true
fi
[[ "$primary" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || { echo 'invalid primary package identity' >&2; exit 2; }
if [[ -n "${RELEASE_GROUP:-}" ]]; then
    group_arguments=("$operation" "$RELEASE_GROUP" "$primary")
    [[ $# != 3 ]] || group_arguments+=("$3")
    exec bash "$ROOT/scripts/ci/cargo-release-group.sh" "${group_arguments[@]}"
fi
if [[ -z "${RELEASE_COMMIT:-}" && ( -e ci/cargo-release-groups.json || -L ci/cargo-release-groups.json ) ]]; then
    echo 'grouped workspace requires explicit RELEASE_GROUP' >&2; exit 1
fi
reader="$ROOT/scripts/ci/read-cargo-workspace-version.sh"

regular_metadata() {
    local directory="$1" path
    shift
    for path in "$@"; do
        [[ -f "$directory/$path" && ! -L "$directory/$path" ]] || {
            echo "release metadata must be regular files: $directory/$path" >&2; return 1;
        }
    done
}

lock_packages() {
    local directory="$1" version="$2"
    "${YQ:-yq}" -p toml -o json '.' "$directory/Cargo.lock" |
        jq -er --arg primary "$primary" --arg version "$version" '
            [.package[] | select(.source == null)] as $local |
            if any($local[]; .name == $primary) and
               all($local[]; .version == $version and (.name | test("^[A-Za-z0-9][A-Za-z0-9_-]*$"))) and
               (($local | map(.name) | unique | length) == ($local | length))
            then $local[].name else error("invalid coordinated workspace lock identities") end'
}

workspace_packages() {
    local metadata="$1" version="$2" names="$3"
    jq -L "$ROOT/scripts/ci" -er --arg primary "$primary" --arg version "$version" --rawfile names "$names" '
        include "cargo-workspace-members";
        coordinated_members($primary; $version) as $owned |
        if (($owned | map(.name) | sort) == ($names | split("\n") | map(select(length > 0)) | sort))
        then true else error("workspace members and admitted lock identities must agree") end' "$metadata" > /dev/null
}

inherited_members() {
    local directory="$1" metadata="$2" manifest
    jq -er '. as $m | .packages[] | select(.id as $id | $m.workspace_members | index($id)) | .manifest_path' "$metadata" |
        while IFS= read -r manifest; do
            case "$manifest" in "$directory"/*) ;; *) echo 'workspace member is outside the selected root' >&2; exit 1 ;; esac
            [[ -f "$manifest" && ! -L "$manifest" ]] || exit 1
            "${YQ:-yq}" -p toml -o json '.' "$manifest" |
                jq -e '.package.version.workspace == true' > /dev/null
        done
}

# Observe and admit one selected workspace independently at every boundary.
# Keep Cargo JSON, diagnostics and child/logger statuses as separate evidence.
admit_workspace_graph() {
    local directory="$1" version="$2" evidence="$3" lock_directory="$4" status
    local metadata_status=(0 0)
    mkdir -p "$evidence" || return
    lock_packages "$lock_directory" "$version" > "$evidence/local-packages" || return
    if (cd "$directory" && cargo metadata --locked --offline --format-version 1) \
        2>&1 > "$evidence/metadata.json" | tee "$evidence/metadata.stderr" >&2; then
        :
    else
        metadata_status=("${PIPESTATUS[@]}")
    fi
    printf '%s\n' "${metadata_status[0]}" > "$evidence/cargo-status" || return
    printf '%s\n' "${metadata_status[1]}" > "$evidence/log-status" || return
    status="${metadata_status[0]}"
    [[ "$status" != 0 ]] || status="${metadata_status[1]}"
    [[ "$status" == 0 ]] || return "$status"
    workspace_packages "$evidence/metadata.json" "$version" "$evidence/local-packages"
}

# shellcheck disable=SC2329 # Callback passed to release_metadata_prepare.
render_cargo_metadata() {
    local original="$1" candidate="$2" source_root package
    local packages=()
    cargo_preparation="${candidate%/*}"
    source_root="$(pwd -P)" || return
    admit_workspace_graph "$source_root" "$RELEASE_PREVIOUS" "$cargo_preparation" "$original" || return
    inherited_members "$source_root" "$cargo_preparation/metadata.json" || return
    "${YQ:-yq}" -p toml -o json '.' "$original/Cargo.toml" > "$cargo_preparation/manifest.json" || return
    jq -n --slurpfile manifest "$cargo_preparation/manifest.json" --slurpfile metadata "$cargo_preparation/metadata.json" '
        {manifest: $manifest[0], members: ($metadata[0] | . as $m |
            [.packages[] | select(.id as $id | $m.workspace_members | index($id)) |
                {name, manifest_path}])}' > "$cargo_preparation/workspace-input.json" || return
    awk -v version="${RELEASE_VERSION:?}" -v previous="$RELEASE_PREVIOUS" -v date="${RELEASE_DATE:?}" \
        -f "$ROOT/scripts/ci/finalize-release-changelog.awk" "$original/CHANGELOG.md" > "$candidate/CHANGELOG.md" || return
    # Live path resolution distinguishes owned members from unrelated aliases.
    # The transaction compares every live input with its original before install.
    "$editor" --max-input-bytes "$limit" manifest Cargo.toml \
        "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "$cargo_preparation/workspace-input.json" > "$candidate/Cargo.toml" || return
    # Independently parse candidate bytes without resolving scratch members.
    "${YQ:-yq}" -p toml -o json '.' "$candidate/Cargo.toml" |
        jq -e --arg version "$RELEASE_VERSION" '.workspace.package.version == $version' > /dev/null || return
    while IFS= read -r package; do packages+=("$package"); done < "$cargo_preparation/local-packages"
    "$editor" --max-input-bytes "$limit" lock "$original/Cargo.lock" \
        "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "${packages[@]}" > "$candidate/Cargo.lock"
}

# shellcheck disable=SC2329 # Callback passed to release_metadata_prepare.
check_prepared_cargo_metadata() {
    local observed_version
    observed_version="$(bash "$reader" Cargo.toml)" || return
    [[ "$observed_version" == "$RELEASE_VERSION" ]] || return 1
    admit_workspace_graph "$PWD" "$RELEASE_VERSION" "$cargo_preparation/prepared" "$PWD"
}

case "$operation" in
    prepare)
        editor="${CARGO_RELEASE_METADATA_EDITOR:?select the prepared released IC Build editor}"
        limit="${CARGO_RELEASE_METADATA_MAX_BYTES:?select a positive per-document byte allowance}"
        [[ "$editor" == /* && -f "$editor" && -x "$editor" && "$limit" =~ ^[1-9][0-9]*$ ]] || exit 2
        regular_metadata . Cargo.toml Cargo.lock CHANGELOG.md
        observed_version="$(bash "$reader" Cargo.toml)"
        [[ "$observed_version" == "${RELEASE_PREVIOUS:?}" ]] || exit 1
        order="$(perl "$ROOT/scripts/ci/compare-stable-versions.pl" "${RELEASE_VERSION:?}" "$observed_version")"
        [[ "$order" == 1 ]] || { echo 'release target must follow the previous version' >&2; exit 1; }
        # shellcheck source=scripts/ci/release-metadata-transaction.sh
        source "$ROOT/scripts/ci/release-metadata-transaction.sh"
        release_metadata_prepare "${TMPDIR:-/tmp}/cargo-release-backup.XXXXXX" \
            render_cargo_metadata check_prepared_cargo_metadata Cargo.toml Cargo.lock CHANGELOG.md
        ;;
    check|check-graph)
        selected_root="$(pwd -P)"
        if [[ -n "${RELEASE_COMMIT:-}" ]]; then
            [[ "$RELEASE_COMMIT" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || exit 1
            object_type="$(git cat-file -t "$RELEASE_COMMIT")"
            [[ "$object_type" == commit ]] || exit 1
        fi
        temporary="$(mktemp -d "${TMPDIR:-/tmp}/cargo-release-check.XXXXXX")"
        complete=false
        finish_check() {
            local status=$?
            [[ "$complete" == true || "$status" != 0 ]] || status=1
            if [[ "$status" == 0 ]]; then rm -rf "$temporary"
            else printf 'failed Cargo metadata check retained: %s\n' "$temporary" >&2; fi
            exit "$status"
        }
        trap finish_check EXIT
        if [[ -n "${RELEASE_COMMIT:-}" ]]; then
            selected_root="$temporary/source"
            mkdir "$selected_root"
            git archive --format=tar "$RELEASE_COMMIT" | tar -xf - -C "$selected_root"
            selected_root="$(cd "$selected_root" && pwd -P)"
        fi
        regular_metadata "$selected_root" Cargo.toml Cargo.lock
        if [[ -e "$selected_root/ci/cargo-release-groups.json" || -L "$selected_root/ci/cargo-release-groups.json" ]]; then
            echo 'selected grouped source requires explicit RELEASE_GROUP' >&2; exit 1
        fi
        mkdir "$temporary/inputs"
        cp -p "$selected_root/Cargo.toml" "$selected_root/Cargo.lock" "$temporary/inputs/"
        printf '%s\n' "$selected_root" > "$temporary/root"
        if [[ "$operation" == check ]]; then
            regular_metadata "$selected_root" CHANGELOG.md
        fi
        if [[ "$check_manifest_order" == true ]]; then
            # Check the same selected source as the graph, using the caller's
            # prepared Cargo tool. Never execute scripts from the export.
            manifest_sort=(sort --workspace --check)
            [[ "$group_manifest_order" != true ]] || manifest_sort+=(--grouped)
            (cd "$selected_root" && cargo "${manifest_sort[@]}") || exit $?
        fi
        observed_version="$(bash "$reader" "$selected_root/Cargo.toml")"
        if [[ "$operation" == check ]]; then
            [[ "$observed_version" == "${RELEASE_VERSION:?}" ]] || exit 1
            awk -v heading="## [$RELEASE_VERSION] - ${RELEASE_DATE:?}" \
                '$0 == heading { count++ } END { if (count != 1) exit 1 }' "$selected_root/CHANGELOG.md"
        fi
        admit_workspace_graph "$selected_root" "$observed_version" "$temporary" "$selected_root"
        complete=true
        ;;
    *) echo 'unknown Cargo metadata operation' >&2; exit 2 ;;
esac
