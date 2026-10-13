#!/usr/bin/env bash
# Sourced by standalone fixtures; no effects until their EXIT trap runs.
# shellcheck disable=SC2154 # Each fixture explicitly marks successful completion.
fixture_finish() {
    local status="$1" directory="$2" label="$3"
    # Bash 3.2 can report zero after nounset before assertions finish.
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then rm -rf -- "$directory"
    else printf '%s: %s\n' "$label" "$directory" >&2; fi
    exit "$status"
}
