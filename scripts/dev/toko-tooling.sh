#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-toko-tooling-snapshot.sh scripts/ci/check-tooling-prerequisites.sh scripts/dev/tool-store.sh scripts/dev/tooling-lock.pl scripts/distribution/registry-package.pl scripts/distribution/refresh-consumer.sh
set -euo pipefail
scratch=''
complete=false
finish() {
    local status=$?
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    if [[ -n "$scratch" ]]; then
        if [[ "$status" == 0 ]]; then rm -rf -- "$scratch" || status=$?;
        else printf 'Tooling installation evidence retained: %s\n' "$scratch" >&2; fi
    fi
    exit "$status"
}
trap finish EXIT
main() {
    local mode="${1:-}" consumer="$PWD" previous_source='' manifest=.toko-tooling.snapshot precise=''
    local script_root records='' record first rest installed_version=unrecorded installed_revision='' installed_source=''
    local lock_record lock_version lock_checksum archive previous_archive='' entrypoints=() options=()
    local preview=false setup=true tool
    case "$mode" in status|update|install|check) shift ;; *) echo 'usage: toko-tooling.sh status|update|install|check [--preview] [--snapshot-only] [--consumer DIR] [--manifest PATH] [--entrypoint PATH] [--source OLD-GIT-CHECKOUT] [--version X.Y.Z]' >&2; return 2 ;; esac
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --preview) preview=true; shift; continue ;;
            --snapshot-only) setup=false; shift; continue ;;
        esac
        [[ $# -ge 2 && -n "$2" ]] || return 2
        case "$1" in
            --consumer) consumer="$2" ;;
            --source) previous_source="$2" ;;
            --manifest) manifest="$2" ;;
            --entrypoint) entrypoints+=(--entrypoint "$2") ;;
            --version) precise="$2" ;;
            *) echo "unknown option: $1" >&2; return 2 ;;
        esac
        shift 2
    done
    [[ ( "$preview" == false && "$setup" == true ) || "$mode" == install || "$mode" == update ]] || {
        echo '--preview and --snapshot-only apply only to install or update' >&2; return 2;
    }
    [[ -z "$precise" || ( "$mode" == update && "$precise" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ) ]] || {
        echo '--version confirms the Cargo selection during update; install uses Cargo.lock unchanged' >&2; return 2;
    }
    case "/$manifest/" in //*|*'/../'*|*'/./'*|*'//'*) echo 'manifest must be a canonical relative path' >&2; return 2 ;; esac
    case "$consumer$previous_source$manifest" in *$'\n'*|*$'\r'*) echo 'paths must not contain line breaks' >&2; return 2 ;; esac
    consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"; consumer="${consumer%/.}"
    script_root="$(cd -P "${BASH_SOURCE[0]%/*}/../.." && printf '%s/.' "$PWD")"; script_root="${script_root%/.}"
    export PATH="$consumer/.tools/host/bin:$PATH"
    bash "$script_root/scripts/ci/check-tooling-prerequisites.sh" selection || return 1
    if [[ "$mode" == install || "$mode" == update ]]; then
        bash "$script_root/scripts/ci/check-tooling-prerequisites.sh" archive || return 1
        for tool in curl git; do
            command -v "$tool" >/dev/null 2>&1 || { echo "Toko Tooling requires $tool on PATH." >&2; return 1; }
        done
        if [[ "$preview" == false && "$setup" == true ]]; then
            command -v make >/dev/null 2>&1 || { echo 'Toko Tooling requires GNU Make for tool setup.' >&2; return 1; }
        fi
    fi
    if [[ -f "$consumer/$manifest" && ! -L "$consumer/$manifest" ]]; then
        records="$(bash "$script_root/scripts/ci/verify-toko-tooling-snapshot.sh" --read-manifest "$consumer/$manifest")" || return 1
        while IFS=$'\t' read -r record first rest; do
            case "$record" in
                '# version') installed_version="$first" ;;
                revision) installed_revision="$first" ;;
                source) installed_source="$first" ;;
            esac
        done <<< "$records"
    elif [[ -e "$consumer/$manifest" || -L "$consumer/$manifest" ]]; then
        echo 'snapshot manifest is not a regular file' >&2; return 1
    fi
    lock_record="$(perl "$script_root/scripts/dev/tooling-lock.pl" "$consumer")" || return 1
    lock_version="$(printf '%s' "$lock_record" | jq -r .version)"
    [[ -z "$precise" || "$precise" == "$lock_version" ]] || { echo "--version must match Cargo selection; edit the root pin and update Cargo.lock first." >&2; return 1; }
    lock_checksum="$(printf '%s' "$lock_record" | jq -r .checksum)"
    printf 'Installed: %s (%s)\nSelected from crates.io: %s (%s)\n' "$installed_version" "$installed_revision" "$lock_version" "$lock_checksum"
    if [[ "$mode" == check || "$mode" == status ]]; then
        [[ -n "$records" ]] || { echo 'No installed snapshot. Run toko-tooling-install with the initial --entrypoint selection.' >&2; return 1; }
        bash "$script_root/scripts/ci/verify-toko-tooling-snapshot.sh" --consumer "$consumer" --manifest "$manifest" || return 1
        if [[ "$installed_source" != https://crates.io/crates/toko-tooling || "$installed_revision" != "$lock_checksum" || "$installed_version" != "$lock_version" ]]; then
            echo 'Cargo.lock selects different toko-tooling bytes. Run make toko-tooling-install before using or deploying this snapshot.' >&2
            [[ "$mode" == status ]] && return 0
            return 1
        fi
        echo 'Every package declaration, Cargo.lock and installed snapshot agree.'
        return 0
    fi
    [[ -n "$records" || ${#entrypoints[@]} -gt 0 ]] || { echo 'Initial installation requires explicit --entrypoint selections.' >&2; return 2; }
    if [[ "$preview" == true ]]; then
        scratch="$(cd -P "${TMPDIR:-/tmp}" && pwd)" || return 1
        case "$scratch/" in "$consumer/"*|"$script_root/"*) echo 'Preview scratch must be outside the consumer and tooling source; set TMPDIR accordingly.' >&2; scratch=''; return 1 ;; esac
        scratch="$(mktemp -d "$scratch/toko-tooling-preview.XXXXXX")" || return 1
        # Preview must not populate a consumer-owned download cache either.
        unset TOKO_TOOLING_DOWNLOAD_CACHE
    else
        [[ ! -L "$consumer/target" && ! -L "$consumer/target/tmp" ]] || { echo 'Tooling scratch directories may not be symlinks.' >&2; return 1; }
        mkdir -p "$consumer/target/tmp"
        scratch="$(mktemp -d "$consumer/target/tmp/toko-tooling-install.XXXXXX")"
    fi
    # shellcheck source=scripts/dev/tool-store.sh
    source "$script_root/scripts/dev/tool-store.sh"
    archive="$scratch/toko-tooling-$lock_version.crate"
    tool_store_download "https://static.crates.io/crates/toko-tooling/toko-tooling-$lock_version.crate" "$archive" "$lock_checksum" || return 1
    perl "$script_root/scripts/distribution/registry-package.pl" extract "$archive" "$lock_checksum" "$lock_version" "$scratch/package" || return 1
    options=()
    [[ "$preview" == false ]] || options+=(--preview)
    if [[ "$installed_source" == https://crates.io/crates/toko-tooling && "$installed_revision" != "$lock_checksum" ]]; then
        previous_archive="$scratch/previous.crate"
        tool_store_download "https://static.crates.io/crates/toko-tooling/toko-tooling-$installed_version.crate" "$previous_archive" "$installed_revision" || return 1
        options+=(--previous-crate "$previous_archive")
    elif [[ -n "$records" && "$installed_source" != https://crates.io/crates/toko-tooling ]]; then
        [[ -n "$previous_source" ]] || { echo 'For one-time Git snapshot migration, set TOKO_TOOLING_SOURCE to the old checkout for ownership proof.' >&2; return 1; }
        options+=(--previous-source "$previous_source")
    fi
    bash "$scratch/package/scripts/distribution/refresh-consumer.sh" --crate "$archive" --checksum "$lock_checksum" \
        --package-version "$lock_version" --consumer "$consumer" --manifest "$manifest" \
        ${entrypoints[@]+"${entrypoints[@]}"} ${options[@]+"${options[@]}"} || return 1
    if [[ "$preview" == true ]]; then
        echo 'Preview complete; consumer files and tools are unchanged.'
        return 0
    fi
    if [[ "$setup" == true ]] && { ! make --no-print-directory -C "$consumer" install-tools || ! make --no-print-directory -C "$consumer" tools-check; }; then
        echo 'Snapshot installed, but tool setup/admission failed. Run make install-tools && make tools-check before using it.' >&2; return 1
    fi
    bash "$scratch/package/scripts/dev/toko-tooling.sh" check --consumer "$consumer" --manifest "$manifest" || return 1
    if [[ "$setup" == true ]]; then
        printf 'toko-tooling %s installed from crates.io and tools admitted.\n' "$lock_version"
    else
        printf 'toko-tooling %s snapshot installed and admitted; tool setup was not run. Run make install-tools and make tools-check before using its tools.\n' "$lock_version"
    fi
}
main "$@"
complete=true
