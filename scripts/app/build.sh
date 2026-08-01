#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

if [ $# -lt 1 ]; then
    echo "usage: build.sh [canister_name] [cargo_package]"
    exit 1
fi

CAN="$1"
PKG="${2:-$CAN}"
ICP_DIR="$ROOT/.icp/local/canisters/$CAN"
WASM_TARGET="$ICP_DIR/$CAN.wasm"

cargo run --manifest-path "$ROOT/Cargo.toml" \
    --package icydb-testing-integration \
    --bin build_fixture_canister \
    -- "$CAN" --build-profile local --profile debug --candid-export on
if [ -n "${ICP_WASM_OUTPUT_PATH:-}" ]; then
    mkdir -p "$(dirname "$ICP_WASM_OUTPUT_PATH")"
    cp -f "$WASM_TARGET" "$ICP_WASM_OUTPUT_PATH"
fi
