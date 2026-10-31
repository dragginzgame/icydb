#!/usr/bin/env bash
# Shared path admission; callers recheck at their own protected effects.
publication_path_option() {
    local option="$1" path="$2" checkout="$3" directory resolved
    [[ -n "$path" && "$path" != -* && "$path" != *$'\r'* && "$path" != *$'\n'* ]] || {
        echo 'publication refused: invalid path option' >&2; return 2;
    }
    directory="$path"
    [[ "$option" != --manifest-path ]] || directory="$(dirname -- "$path")"
    resolved="$(CDPATH='' cd -P -- "$directory" && printf '%s/.' "$PWD")" || return
    case "$resolved" in *$'\r'*|*$'\n'*) echo 'publication refused: resolved path contains CR/LF' >&2; return 2 ;; esac
    if [[ "$option" == --manifest-path ]]; then
        [[ -f "$path" && ! -L "$path" && "${resolved%/.}/" == "$checkout/"* ]] || {
            echo 'publication refused: manifest must be a regular source file inside the selected checkout' >&2; return 2;
        }
    fi
}
