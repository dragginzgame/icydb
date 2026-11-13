#!/usr/bin/env bash
set -euo pipefail

# Admit the saved source's tool selection before consulting prepared tools.
# Recovery must not use a newer or partially prepared working lockfile.
[[ $# == 1 && ( "$1" == HEAD || "$1" =~ ^[0-9a-f]{40}$ ) ]] || exit 2
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$root/.tools/host/bin:$PATH"
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
# Follow the saved source's workspace owner, including pre-split recovery.
policy="$(git -C "$root" show "$1:ci/cargo-release-groups.json")" || exit 1
lock="$(jq -er 'if .qualification_workspace == "qualification" then "qualification/Cargo.lock" else "Cargo.lock" end' <<< "$policy")" || exit 1
git -C "$root" show "$1:$lock" > "$selection" || exit 1
# The saved Build version owns its binary feature contract. Earlier releases
# supplied the editor without `cli`; recovery must preserve their exact recipe.
version="$("${YQ:-yq}" -p toml -o json '.' "$selection" |
    jq -er '[.package[] | select(.name == "ic-build")] |
        if length == 1 then .[0].version else error("expected one saved IC Build selection") end')" || exit 1
order="$(perl "$root/vendor/toko-tooling/scripts/ci/compare-stable-versions.pl" "$version" 0.63.0)" || exit 1
features=()
if [[ "$order" != -1 ]]; then features=(--features cli); fi
editor="$(bash "$root/vendor/toko-tooling/scripts/dev/install-rust-tools.sh" --consumer "$root" \
    --package ic-build --lockfile "$selection" --bin ic-build-release-metadata \
    --profile debug ${features[@]+"${features[@]}"} --check)" || exit $?
printf '%s\n' "$editor"
complete=true
