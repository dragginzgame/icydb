#!/usr/bin/env bash
set -euo pipefail

# Project the locked Testkit package into Shared's selected Cargo installation.
# Shared owns receipt/byte admission; Testkit owns server setup and admission.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ "$#" != 0 && ( "$#" != 1 || "$1" != --check ) ]]; then
  echo 'usage: testkit-runner.sh [--check]' >&2
  exit 2
fi
export PATH="$ROOT/.tools/host/bin:$PATH"
export CARGO_HOME="$ROOT/.cache/cargo/icydb"
metadata="$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --locked --offline --format-version 1)"
version="$(printf '%s\n' "$metadata" | jq -er \
  '[.packages[] | select(.name == "ic-testkit") | .version] | unique | select(length == 1) | .[0]')"
if bash "$ROOT/scripts/dev/install-rust-tools.sh" --consumer "$ROOT" \
  --package ic-testkit --version "$version" --bin ic-testkit-server --profile release "$@"; then
  exit 0
else
  status=$?
  printf 'Prepare the locked Testkit selection with: make install-testkit\n' >&2
  exit "$status"
fi
