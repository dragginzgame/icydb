#!/usr/bin/env bash
set -euo pipefail

# The public package owns its compiler promise; the workspace's internal floor
# and the repository toolchain must not substitute for that minimum.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
metadata="$(cargo metadata --locked --offline --no-deps --format-version 1)"
expected="$(jq -er '.packages[] | select(.name == "icydb") | .rust_version' <<< "$metadata")"
for tool in rustc cargo; do
  identity="$("$tool" --version)"
  printf '%s\n' "$identity"
  read -r _ actual _ <<< "$identity"
  if [[ "$actual" != "$expected" ]]; then
    printf 'Public MSRV requires %s %s; selected version is %s.\n' "$tool" "$expected" "$actual" >&2
    exit 1
  fi
done

# Compile only the public library graph, avoiding internal test/development
# packages with the higher workspace floor. Check isolated and combined features.
for target in x86_64-unknown-linux-gnu wasm32-unknown-unknown; do
  cargo check --locked --offline --lib -p icydb --no-default-features --target "$target"
  for features in sql migration metrics sql,migration,metrics; do
    cargo check --locked --offline --lib -p icydb --no-default-features --target "$target" --features "$features"
  done
done
