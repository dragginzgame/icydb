#!/usr/bin/env bash
# Sourced by standalone fixtures; no effects until their EXIT trap runs.
# shellcheck disable=SC2154 # Each fixture explicitly marks successful completion.
fixture_finish() {
    local status="$1" directory="$2" label="$3"
    # Bash 3.2 can report zero after nounset before assertions finish.
    [[ "${fixture_complete:-false}" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then
        # Callers anchor their created directory before changing cwd. Never let
        # a relative path or empty/root selection turn cleanup into another effect.
        if [[ "$directory" != /* || "$directory" == / ]]; then status=1
        else
            rm -rf -- "$directory" || status=$?
            [[ ! -e "$directory" && ! -L "$directory" ]] || { [[ "$status" != 0 ]] || status=1; }
        fi
    fi
    if [[ "$status" != 0 ]]; then
        # Reporting must not replace the original operation/cleanup failure.
        printf '%s: %s\n' "$label" "$directory" >&2 || :
    fi
    exit "$status"
}
