#!/usr/bin/env bash

# Keep IcyDB cargo state repo-local so sibling repos on the same filesystem do
# not contend on a shared cargo home lock or build target directory.
if [[ -z "${ROOT:-}" ]]; then
    ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
fi

export ROOT
export CARGO_HOME="${CARGO_HOME:-$ROOT/.cache/cargo/icydb}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/icydb}"
