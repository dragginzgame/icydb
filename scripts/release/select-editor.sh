#!/usr/bin/env bash
set -euo pipefail

# Admit the saved source's tool selection before consulting prepared tools.
# Recovery must not use a newer or partially prepared working lockfile.
[[ $# == 1 && ( "$1" == HEAD || "$1" =~ ^[0-9a-f]{40}$ ) ]] || exit 2
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
selection="$(mktemp "${TMPDIR:-/tmp}/icydb-release-editor.XXXXXX")" || exit 1
complete=false
finish() {
    local status=$?
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then rm -f "$selection"
    else printf 'Release editor input retained: %s\n' "$selection" >&2; fi
    exit "$status"
}
trap finish EXIT
git -C "$root" show "$1:Cargo.lock" > "$selection" || exit 1
editor="$(bash "$root/scripts/dev/install-rust-tools.sh" --consumer "$root" \
    --package ic-build --lockfile "$selection" --bin ic-build-release-metadata \
    --profile debug --check)" || exit $?
printf '%s\n' "$editor"
complete=true
