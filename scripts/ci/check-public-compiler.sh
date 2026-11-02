#!/usr/bin/env bash
set -euo pipefail

# Qualify the isolated public graph on the repository's admitted compiler.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
bash "$ROOT/scripts/ci/check-rust-compiler.sh"

# Keep library-only isolated feature checks: workspace feature unification
# cannot substitute for a public consumer's native and Wasm graph.
for target in x86_64-unknown-linux-gnu wasm32-unknown-unknown; do
  cargo check --locked --offline --lib -p icydb --no-default-features --target "$target"
  for features in sql migration metrics sql,migration,metrics; do
    cargo check --locked --offline --lib -p icydb --no-default-features --target "$target" --features "$features"
  done
done
