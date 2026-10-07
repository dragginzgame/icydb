#!/usr/bin/env bash
set -euo pipefail

# Consumers own the reviewed pin, selected toolchain and workspace roster.
usage() { echo 'usage: check-format-tools.sh <cargo-sort-version> [<cargo-executable>]' >&2; }
if [[ ${1:-} == --help || ${1:-} == -h ]]; then usage; exit 0; fi
[[ $# -ge 1 && $# -le 2 && "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
    usage; exit 2;
}
version="$1"
cargo_bin="${2:-cargo}"
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
if ! actual="$("$cargo_bin" sort --version)" || [[ "$actual" != "cargo-sort $version" ]]; then
    echo "Formatting requires prepared cargo-sort $version; install that version with --locked during explicit setup." >&2
    exit 1
fi
if ! "$cargo_bin" fmt --version >/dev/null; then
    echo 'Formatting requires prepared rustfmt for the selected toolchain; prepare it during explicit setup.' >&2
    exit 1
fi
