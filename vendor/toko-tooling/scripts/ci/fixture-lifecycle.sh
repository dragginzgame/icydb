#!/usr/bin/env bash
# Sourced by standalone fixtures; effects occur only on explicit helper calls.
# Allocate and anchor one caller-selected fixture directory before cwd changes.
# Failure after allocation retains the directory and reports its original path.
fixture_create() {
    [[ $# == 1 ]] || return 2
    case "$1" in ''|-*|*$'\r'*|*$'\n'*) return 2 ;; esac
    local created directory status
    created="$(mktemp -d "$1")" || return
    directory="$(CDPATH='' cd -P -- "$created" && printf '%s/.' "$PWD")" || {
        status=$?
        printf 'Fixture allocation retained: %s\n' "$created" >&2 || :
        return "$status"
    }
    case "$directory" in
        *$'\r'*|*$'\n'*)
            printf 'Fixture physical directory contains CR/LF; allocation retained: %s\n' "$created" >&2 || :
            return 2 ;;
    esac
    printf '%s\n' "${directory%/.}"
}

# Replay caller-selected existing logs without replacing the saved failure status.
fixture_replay_logs() {
    local log
    for log in "$@"; do
        if [[ -f "$log" ]]; then cat -- "$log" >&2 || :; fi
    done
    # Diagnostics are best effort and must not replace a caller's saved status.
    return 0
}

# Completion admission only; callers retain evidence and cleanup policy.
fixture_completion_status() {
    [[ $# == 2 ]] || return 2
    local status="$1" complete="$2"
    # Reject invalid statuses rather than letting shell return wrap them to zero.
    case "$status" in
        0|[1-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5]) ;;
        *) return 2 ;;
    esac
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    return "$status"
}

# shellcheck disable=SC2154 # Each fixture explicitly marks successful completion.
fixture_finish() {
    local status="$1" directory="$2" label="$3"
    # Bash 3.2 can report zero after nounset before assertions finish.
    fixture_completion_status "$status" "${fixture_complete:-false}" || status=$?
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
