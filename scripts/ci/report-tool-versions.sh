#!/usr/bin/env bash
# Shared companions: scripts/dev/install-host-tools.sh scripts/dev/install-rust-tools.sh scripts/dev/install-ic-tools.sh scripts/ci/tool-catalog-records.pl scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Observe only admitted common selections. Compiler and product tools stay local.
root="${BASH_SOURCE[0]}"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "${root%/*}/../.." && printf '%s/.' "$PWD")"
root="${root%/.}"
if [[ $# != 1 && $# != 2 && $# != 4 ]]; then
    echo 'usage: report-tool-versions.sh CONSUMER [VERSIONS [IC_PINS IC_TOOLS]]' >&2
    exit 2
fi
consumer="$1"
versions="${2:-$root/ci/tool-versions.env}"
kinds=(host rust)
pins="${3:-}"
ic_tools=()
if [[ $# == 4 ]]; then
    [[ -n "$4" && "$4" != *$'\r'* && "$4" != *$'\n'* ]] || exit 2
    read -r -a ic_tools <<< "$4"
    [[ ${#ic_tools[@]} != 0 ]] || exit 2
    for tool in "${ic_tools[@]}"; do
        case "$tool" in quill|icp|didc|ic-wasm|wasm-opt) ;; *) exit 2 ;; esac
    done
    kinds+=(ic)
fi
for kind in "${kinds[@]}"; do
    catalog="$versions"
    option=--versions
    if [[ "$kind" == ic ]]; then catalog="$pins"; option=--pins; fi
    # Capture parser and admission status before dispatching any version probe.
    records="$(perl "$root/scripts/ci/tool-catalog-records.pl" "$kind" "$catalog")"
    printf 'selection=%s\n' "$kind"
    status=0
    checker=(bash "$root/scripts/dev/install-$kind-tools.sh" --consumer "$consumer" "$option" "$catalog")
    if [[ "$kind" == ic ]]; then
        for tool in "${ic_tools[@]}"; do checker+=(--tool "$tool"); done
    fi
    directory="$("${checker[@]}" --check)" || status=$?
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
        if [[ "$kind" == ic ]]; then
            selected=false
            for requested in "${ic_tools[@]}"; do [[ "$tool" != "$requested" ]] || selected=true; done
            [[ "$selected" == true ]] || continue
        fi
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
