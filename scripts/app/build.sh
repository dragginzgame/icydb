#!/bin/bash

# don't allow errors
set -e

# Resolve the repo and scripts roots locally so this entrypoint does not rely
# on a shared env shim.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
SCRIPTS="$ROOT/scripts"

export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"
cd "$SCRIPTS"

# Check if required arguments were provided
if [ $# -eq 0 ]; then
    echo "usage: build.sh [canister_name] [cargo_package]"
    exit 1
fi
CAN="$1"
PKG="${2:-$CAN}"

#
# Build Wasm
#

mkdir -p "$ROOT/.dfx/local/canisters/$CAN"
WASM_TARGET="$ROOT/.dfx/local/canisters/$CAN/$CAN.wasm"

cargo build --target wasm32-unknown-unknown -p "$PKG"
cp -f "$CARGO_TARGET_DIR/wasm32-unknown-unknown/debug/$PKG.wasm" "$WASM_TARGET"

# extract candid
candid-extractor "$WASM_TARGET" \
    > "$ROOT/.dfx/local/canisters/$CAN/${CAN}.did"
