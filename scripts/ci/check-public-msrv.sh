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

# External expansion is a separate compiler boundary from the defining library.
# Cargo derives each disposable consumer lock from the selected workspace lock;
# metadata membership prevents an offline resolution from selecting another graph.
scratch="$(mktemp -d "${TMPDIR:-$ROOT/.cache}/icydb-endpoint-msrv.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$scratch"
  else printf 'Failed external MSRV consumer retained: %s\n' "$scratch" >&2; fi
}
trap finish EXIT
cargo metadata --locked --offline --format-version 1 > "$scratch/workspace.json"
CARGO_TARGET_DIR="$(jq -er '.target_directory' "$scratch/workspace.json")"
export CARGO_TARGET_DIR
facade_path="$(jq -n --arg path "$ROOT/crates/icydb" '$path')"
fixture_path="$(jq -n --arg path "$ROOT/crates/icydb/tests/pass/endpoint_msrv.rs" '$path')"
candid_requirement="$(jq -er '.packages[] | select(.name == "icydb") |
  .dependencies[] | select(.name == "candid") | .req' "$scratch/workspace.json")"
cdk_requirement="$(jq -er '.packages[] | select(.name == "icydb") |
  .dependencies[] | select(.name == "ic-cdk") | .req' "$scratch/workspace.json")"
for dependency in icydb runtime_api; do
  consumer="$scratch/$dependency"
  mkdir -p "$consumer/src"
  cat > "$consumer/Cargo.toml" <<TOML
[package]
name = "icydb-endpoint-msrv-probe"
version = "0.0.0"
edition = "2024"
rust-version = "$expected"
[lib]
crate-type = ["cdylib", "rlib"]
[features]
sql = ["$dependency/sql"]
migration = ["$dependency/migration"]
metrics = ["$dependency/metrics"]
[dependencies]
$dependency = { package = "icydb", path = $facade_path, default-features = false }
candid = "$candid_requirement"
ic-cdk = "$cdk_requirement"
[workspace]
TOML
  if [[ "$dependency" == icydb ]]; then
    printf 'extern crate icydb as runtime_api;\n' > "$consumer/src/lib.rs"
  fi
  printf 'include!(%s);\n' "$fixture_path" >> "$consumer/src/lib.rs"
  cp "$ROOT/Cargo.lock" "$consumer/Cargo.lock"
  cargo metadata --offline --manifest-path "$consumer/Cargo.toml" --all-features --format-version 1 \
    > "$consumer/metadata.json"
  jq -e --slurpfile selected "$scratch/workspace.json" '
    all(.packages[] | select(.source != null);
      .id as $id | any($selected[0].packages[]; .id == $id))
  ' "$consumer/metadata.json" > /dev/null
  for target in x86_64-unknown-linux-gnu wasm32-unknown-unknown; do
    cargo check --locked --offline --manifest-path "$consumer/Cargo.toml" --target "$target"
    cargo check --locked --offline --manifest-path "$consumer/Cargo.toml" --target "$target" \
      --features sql,migration,metrics
  done
done

# Compile only the public library graph, avoiding internal test/development
# packages with the higher workspace floor. Check isolated and combined features.
for target in x86_64-unknown-linux-gnu wasm32-unknown-unknown; do
  cargo check --locked --offline --lib -p icydb --no-default-features --target "$target"
  for features in sql migration metrics sql,migration,metrics; do
    cargo check --locked --offline --lib -p icydb --no-default-features --target "$target" --features "$features"
  done
done
