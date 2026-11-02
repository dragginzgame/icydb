#!/usr/bin/env bash
set -euo pipefail

# Consumer admission: the declared floor and pinned toolchain are one promise.
# Shared owns TOML bootstrap parsing; this check never installs a compiler.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
expected="$(bash "$ROOT/scripts/ci/read-cargo-workspace-rust-version.sh" "$ROOT/Cargo.toml")"
selected="$("${YQ:-yq}" -p toml -o json '.toolchain.channel' "$ROOT/rust-toolchain.toml" | jq -er 'select(type == "string")')"
if [[ "$selected" != "$expected" ]]; then
  printf 'Pinned Rust compiler %s differs from workspace support floor %s.\n' "$selected" "$expected" >&2
  exit 1
fi
for tool in rustc cargo; do
  identity="$("$tool" --version)"
  printf '%s\n' "$identity"
  read -r _ actual _ <<< "$identity"
  if [[ "$actual" != "$expected" ]]; then
    printf 'Rust policy requires %s %s; selected version is %s.\n' "$tool" "$expected" "$actual" >&2
    exit 1
  fi
done
