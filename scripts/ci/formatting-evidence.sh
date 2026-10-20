#!/usr/bin/env bash
# Sourced selection only; callers own archive layout, custody and upload.
# Populate root/relative-path pairs without subshell status or filename parsing.
select_formatting_evidence() {
    formatting_evidence=()
    [[ $# == 1 && "$1" == /* && -d "$1" ]] || return 2
    local root="${1%/}" path
    [[ -n "$root" ]] || root=/
    for path in "$root"/formatting.*; do
        if [[ -f "$path" || -L "$path" ]]; then
            formatting_evidence+=("$root" "${path##*/}")
        fi
    done
}
