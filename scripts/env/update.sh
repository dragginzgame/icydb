#!/bin/bash
set -e

# rustup
rustup update

# cargo
cargo install \
    cargo-audit cargo-bloat cargo-deny cargo-expand cargo-machete \
    cargo-llvm-lines cargo-sort cargo-tarpaulin cargo-sort-derives \
    ripgrep \
    candid-extractor ic-wasm

# cleanup
cargo audit

# update last
cargo update --verbose

# dfx
dfxvm self update
