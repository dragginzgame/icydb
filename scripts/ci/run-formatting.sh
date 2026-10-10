#!/usr/bin/env bash
set -euo pipefail

# Presentation only: the caller owns formatter selection, ordering and policy.
if [[ $# -lt 2 || ( "$1" != --check && "$1" != --write ) ]]; then
    echo 'usage: run-formatting.sh --check|--write COMMAND [ARG ...]' >&2
    exit 2
fi
label='Checking formatting'
[[ "$1" != --write ]] || label=Formatting
shift
log="$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/formatting.XXXXXX")"
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then
        printf 'ok\n'
        rm -f "$log"
    else
        printf 'FAILED (exit %s)\nDetails: %q\n' "$status" "$log"
    fi
    exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s... ' "$label"
"$@" > "$log" 2>&1
