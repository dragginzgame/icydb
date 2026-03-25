#!/bin/bash

# don't allow errors
set -e

# Set up environment
source "$(dirname "$0")/../env.sh"
source "$(dirname "$0")/../env/cargo-local.sh"
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
