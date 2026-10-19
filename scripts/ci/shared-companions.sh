#!/usr/bin/env bash
# Read declarations without executing payloads or spawning a per-file parser.
# The caller validates each returned path for its own custody boundary.
read_shared_companions() {
    local declaration=''
    {
        IFS= read -r declaration || :
        IFS= read -r declaration || :
    } < "$1"
    if [[ "$declaration" == '# Shared companions: '* ]]; then
        declaration="${declaration#'# Shared companions: '}"
    elif [[ "$2" == *.md && "$declaration" == '<!-- Shared companions: '*' -->' ]]; then
        declaration="${declaration#'<!-- Shared companions: '}"
        declaration="${declaration%' -->'}"
    else
        declaration=''
    fi
    # Bash 3.2 consumers read this array immediately after the call.
    shared_companions=()
    # shellcheck disable=SC2034
    read -r -a shared_companions <<< "$declaration"
}
