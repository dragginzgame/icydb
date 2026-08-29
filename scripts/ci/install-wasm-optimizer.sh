#!/usr/bin/env bash
set -euo pipefail

CANIC_CLI_VERSION="0.109.19"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cargo install --locked canic-cli --version "$CANIC_CLI_VERSION"
wasm_opt_bin="$(canic toolchain install)"
wasm_opt_dir="$(dirname "$wasm_opt_bin")"
export PATH="$wasm_opt_dir:$PATH"
if [[ -n "${GITHUB_PATH:-}" ]]; then
  printf '%s\n' "$wasm_opt_dir" >> "$GITHUB_PATH"
fi
bash "$ROOT/scripts/ci/verify-wasm-optimizer.sh"
