#!/usr/bin/env bash
set -euo pipefail

# Read-only bootstrap projection: no Cargo, Rustup, dependency resolution or setup.
[[ $# == 1 && -f "$1" ]] || {
    echo 'usage: read-cargo-workspace-rust-version.sh <manifest>' >&2; exit 2;
}
file="$1"
case "$file$PWD" in *$'\n'*|*$'\r'*) echo 'Rust policy path contains CR/LF' >&2; exit 2 ;; esac
[[ "$file" == /* ]] || file="$PWD/$file"
directory="$(cd -P -- "${file%/*}" && printf '%s/.' "$PWD")"
case "$directory" in *$'\n'*|*$'\r'*) echo 'resolved Rust policy directory contains CR/LF' >&2; exit 2 ;; esac
# Capture each observation before publishing: plausible output cannot hide failure.
document="$("${YQ:-yq}" -p toml -o json '.' "$file")"
version="$(printf '%s' "$document" | jq -ers '
    if length != 1 then error("expected one Rust policy document") else .[0] end |
    select(type == "object") |
    .workspace.package."rust-version" |
    select(type == "string" and (test("[\r\n]") | not)) |
    select(test("^[0-9]+\\.[0-9]+(\\.[0-9]+)?$"))
')" || { status=$?; echo 'Rust policy has no valid workspace Rust floor' >&2; exit "$status"; }
printf '%s\n' "$version"
