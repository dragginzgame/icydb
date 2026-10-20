#!/usr/bin/env bash
# Shared companions: scripts/dev/install-host-tools.sh scripts/dev/install-rust-tools.sh scripts/dev/install-ic-tools.sh scripts/ci/tool-catalog-records.pl scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Observe only admitted common selections. Compiler and product tools stay local.
root="${BASH_SOURCE[0]}"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "${root%/*}/../.." && printf '%s/.' "$PWD")"
root="${root%/.}"
if [[ $# -lt 1 || $# -gt 3 ]]; then
    echo 'usage: report-tool-versions.sh CONSUMER [VERSIONS [IC_PINS]]' >&2
    exit 2
fi
consumer="$1"
versions="${2:-$root/ci/tool-versions.env}"
kinds=(host rust)
pins="${3:-}"
[[ $# != 3 ]] || kinds+=(ic)
for kind in "${kinds[@]}"; do
    catalog="$versions"
    option=--versions
    if [[ "$kind" == ic ]]; then catalog="$pins"; option=--pins; fi
    # Capture parser and admission status before dispatching any version probe.
    records="$(perl "$root/scripts/ci/tool-catalog-records.pl" "$kind" "$catalog")"
    printf 'selection=%s\n' "$kind"
    status=0
    directory="$(bash "$root/scripts/dev/install-$kind-tools.sh" --consumer "$consumer" "$option" "$catalog" --check)" || status=$?
    if [[ "$status" != 0 ]]; then
        [[ -z "$directory" ]] || printf '%s\n' "$directory"
        exit "$status"
    fi
    [[ -d "$directory" && "$directory" == /* && "$directory" != *$'\n'* && "$directory" != *$'\r'* ]] || {
        echo 'admission returned an invalid executable directory' >&2; exit 1;
    }
    # Catalog records repeat tools across supported hosts; report each once.
    previous=''
    while IFS=$'\t' read -r tool _; do
        [[ "$tool" != "$previous" ]] || continue
        previous="$tool"
        case "$tool" in
            RIPGREP) command=rg ;;
            *) command="$(printf '%s' "$tool" | tr '[:upper:]_' '[:lower:]-')" ;;
        esac
        executable="$directory/$command"
        digest="$(bash "$root/scripts/ci/verify-file-checksum.sh" --print sha256 "$executable")"
        printf 'tool=%s executable=%q sha256=%s\n' "$command" "$executable" "$digest"
        args=(--version)
        [[ "$command" != cargo-sort-derives ]] || args=(sort-derives --version)
        # Keep partial stdout/stderr and the executable's failure status intact.
        "$executable" "${args[@]}"
    done <<< "$records"
done
