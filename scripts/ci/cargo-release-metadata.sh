#!/usr/bin/env bash
# Shared companions: scripts/ci/read-cargo-workspace-version.sh scripts/ci/rewrite-local-lock-versions.pl scripts/ci/finalize-release-changelog.awk scripts/ci/cargo-workspace-members.jq scripts/ci/compare-stable-versions.pl scripts/ci/render-cargo-workspace-version.pl
set -euo pipefail

# Coordinated workspace versions only. Consumer adapters retain source admission,
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

case "$operation" in
    prepare)
        regular_metadata . Cargo.toml Cargo.lock CHANGELOG.md
        observed_version="$(bash "$reader" Cargo.toml)"
        [[ "$observed_version" == "${RELEASE_PREVIOUS:?}" ]] || exit 1
        order="$(perl "$ROOT/scripts/ci/compare-stable-versions.pl" "${RELEASE_VERSION:?}" "$observed_version")"
        [[ "$order" == 1 ]] || { echo 'release target must follow the previous version' >&2; exit 1; }
        backup="$(mktemp -d "${TMPDIR:-/tmp}/cargo-release-backup.XXXXXX")"
        files=(Cargo.toml Cargo.lock CHANGELOG.md)
        for path in "${files[@]}"; do cp -p "$path" "$backup/$path"; done
        complete=false
        # Print custody before the first mutating command, including SIGKILL cases.
        printf 'Cargo release originals retained: %s\n' "$backup" >&2
        cleanup() {
            local status=$? path restore_failed=false
            trap - EXIT
            if [[ "$complete" != true ]]; then
                [[ "$status" != 0 ]] || status=1
                for path in "${files[@]}"; do
                    if ! cp -p "$backup/$path" "$path"; then
                        printf 'metadata restore failed: %s\n' "$path" >&2
                        restore_failed=true
                    fi
                done
                if [[ "$restore_failed" == true ]]; then
                    printf 'metadata restoration incomplete; originals retained: %s\n' "$backup" >&2
                    exit 1
                fi
                printf 'failed Cargo preparation; restoring release metadata; evidence retained: %s\n' "$backup" >&2
            else
                rm -rf "$backup"
            fi
            exit "$status"
        }
        trap cleanup EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
        lock_packages . "$RELEASE_PREVIOUS" > "$backup/local-packages"
        cargo metadata --locked --offline --format-version 1 > "$backup/metadata.json"
        workspace_packages "$backup/metadata.json" "$RELEASE_PREVIOUS" "$backup/local-packages"
        inherited_members "$(pwd -P)" "$backup/metadata.json"
        "${YQ:-yq}" -p toml -o json '.' Cargo.toml > "$backup/manifest.json"
        jq -n --slurpfile manifest "$backup/manifest.json" --slurpfile metadata "$backup/metadata.json" '
            {manifest: $manifest[0], members: ($metadata[0] | . as $m |
                [.packages[] | select(.id as $id | $m.workspace_members | index($id)) |
                    {name, manifest_path}])}' > "$backup/workspace-input.json"
        awk -v version="${RELEASE_VERSION:?}" -v previous="$RELEASE_PREVIOUS" -v date="${RELEASE_DATE:?}" \
            -f "$ROOT/scripts/ci/finalize-release-changelog.awk" CHANGELOG.md > "$backup/candidate.notes"
        perl "$ROOT/scripts/ci/render-cargo-workspace-version.pl" Cargo.toml \
            "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "$backup/workspace-input.json" > "$backup/candidate.manifest"
        # Independently parse candidate bytes without resolving scratch members.
        "${YQ:-yq}" -p toml -o json '.' "$backup/candidate.manifest" | \
            jq -e --arg version "$RELEASE_VERSION" '.workspace.package.version == $version' > /dev/null
        cat "$backup/candidate.manifest" > Cargo.toml
        packages=()
        while IFS= read -r package; do packages+=("$package"); done < "$backup/local-packages"
        perl "$ROOT/scripts/ci/rewrite-local-lock-versions.pl" "$backup/Cargo.lock" \
            "$RELEASE_PREVIOUS" "$RELEASE_VERSION" "${packages[@]}" > "$backup/candidate.lock"
        cat "$backup/candidate.lock" > Cargo.lock
        cat "$backup/candidate.notes" > CHANGELOG.md
        cargo metadata --locked --offline --format-version 1 > "$backup/prepared.json"
        observed_version="$(bash "$reader" Cargo.toml)"
        [[ "$observed_version" == "$RELEASE_VERSION" ]] || exit 1
        lock_packages . "$RELEASE_VERSION" > "$backup/prepared-packages"
        workspace_packages "$backup/prepared.json" "$RELEASE_VERSION" "$backup/prepared-packages"
        complete=true
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
        fi
        regular_metadata "$selected_root" Cargo.toml Cargo.lock
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
        lock_packages "$selected_root" "$observed_version" > "$temporary/local-packages"
        # Bash 3.2 can lose an unguarded subshell failure before the next command.
        (cd "$selected_root" && cargo metadata --locked --offline --format-version 1) > "$temporary/metadata.json" || exit $?
        workspace_packages "$temporary/metadata.json" "$observed_version" "$temporary/local-packages"
        complete=true
        ;;
    *) echo 'unknown Cargo metadata operation' >&2; exit 2 ;;
esac
