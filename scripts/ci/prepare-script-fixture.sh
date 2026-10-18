#!/usr/bin/env bash
# Shared companions: scripts/ci/shared-companions.sh
set -euo pipefail
# Copy explicit roots and their declared closure into a caller-owned fixture.
[[ $# -ge 3 && -d "$1" && -d "$2" ]] || {
    echo 'usage: prepare-script-fixture.sh SOURCE EXISTING-FIXTURE ROOT...' >&2; exit 2;
}
fixture_fail() { echo "script fixture refused: $*" >&2; exit 2; }
source_path="$1" fixture_path="$2"
shift 2
for directory in "$source_path" "$fixture_path"; do
    case "$directory" in *$'\n'*|*$'\r'*) fixture_fail 'directory contains CR/LF' ;; esac
done
source_root="$(CDPATH='' cd -P -- "$source_path" && printf '%s/.' "$PWD")"
source_root="${source_root%/.}"
fixture_root="$(CDPATH='' cd -P -- "$fixture_path" && printf '%s/.' "$PWD")"
fixture_root="${fixture_root%/.}"
for directory in "$source_root" "$fixture_root"; do
    case "$directory" in *$'\n'*|*$'\r'*) fixture_fail 'resolved directory contains CR/LF' ;; esac
done
case "$source_root/" in "$fixture_root/"*) fixture_fail 'roots overlap' ;; esac
case "$fixture_root/" in "$source_root/"*) fixture_fail 'roots overlap' ;; esac
helper="${BASH_SOURCE[0]}"
[[ "$helper" == /* ]] || helper="$PWD/$helper"
# shellcheck source=scripts/ci/shared-companions.sh
source "${helper%/*}/shared-companions.sh"

fixture_admit_path() {
    local path="$1" parent component cursor
    [[ -n "$path" && "$path" != /* ]] || fixture_fail "expected relative path: $path"
    case "/$path/" in *'/../'*|*'/./'*|*'//'*) fixture_fail "noncanonical path: $path" ;; esac
    case "$path" in *$'\n'*|*$'\r'*|*$'\t'*) fixture_fail 'path contains control character' ;; esac
    parent="${path%/*}"
    if [[ "$parent" != "$path" ]]; then
        for cursor in "$source_root" "$fixture_root"; do
            while IFS= read -r component; do
                cursor+="/$component"
                [[ ! -L "$cursor" ]] || fixture_fail "unsafe parent: $path"
                if [[ -e "$cursor" ]]; then
                    [[ -d "$cursor" && -x "$cursor" ]] || fixture_fail "unsearchable parent: $path"
                elif [[ -d "${cursor%/*}" ]]; then
                    [[ -w "${cursor%/*}" ]] || fixture_fail "unwritable parent: $path"
                fi
            done <<< "${parent//\//$'\n'}"
        done
    fi
    [[ -f "$source_root/$path" && -r "$source_root/$path" && ! -L "$source_root/$path" ]] || fixture_fail "missing regular source: $path"
    [[ ! -L "$fixture_root/$path" ]] || fixture_fail "unsafe destination: $path"
    if [[ -e "$fixture_root/$path" ]]; then
        [[ -f "$fixture_root/$path" && -w "$fixture_root/$path" ]] || fixture_fail "unwritable destination: $path"
    elif [[ -d "${fixture_root}/${path%/*}" && "$path" == */* ]]; then
        [[ -w "${fixture_root}/${path%/*}" ]] || fixture_fail "unwritable destination parent: $path"
    elif [[ "$path" != */* ]]; then
        [[ -w "$fixture_root" ]] || fixture_fail "unwritable destination root: $path"
    fi
}

payloads=()
for path in "$@"; do
    selected=false
    for existing in ${payloads[@]+"${payloads[@]}"}; do [[ "$existing" != "$path" ]] || selected=true; done
    [[ "$selected" == true ]] || payloads+=("$path")
done
cursor=0
# Admit the complete graph and all outputs before making any filesystem change.
while [[ "$cursor" -lt "${#payloads[@]}" ]]; do
    path="${payloads[$cursor]}"
    fixture_admit_path "$path"
    read_shared_companions "$source_root/$path" "$path"
    for companion in ${shared_companions[@]+"${shared_companions[@]}"}; do
        selected=false
        for existing in "${payloads[@]}"; do [[ "$existing" != "$companion" ]] || selected=true; done
        [[ "$selected" == true ]] || payloads+=("$companion")
    done
    cursor=$((cursor + 1))
done
for path in "${payloads[@]}"; do
    [[ "$path" != */* ]] || mkdir -p "$fixture_root/${path%/*}"
    cp -p "$source_root/$path" "$fixture_root/$path"
done
