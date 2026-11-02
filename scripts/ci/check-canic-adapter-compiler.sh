#!/usr/bin/env bash
set -euo pipefail

# Qualify the isolated public graph on the repository's admitted compiler.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
bash "$ROOT/scripts/ci/check-rust-compiler.sh"

expected="$(bash "$ROOT/scripts/ci/read-cargo-workspace-rust-version.sh" "$ROOT/Cargo.toml")"

# External expansion is a separate compiler boundary from the defining library.
# Cargo derives each disposable consumer lock from the selected workspace lock;
# metadata membership prevents an offline resolution from selecting another graph.
scratch="$(mktemp -d "${TMPDIR:-$ROOT/.cache}/icydb-endpoint-compiler.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$scratch"
  else printf 'Failed external compiler consumer retained: %s\n' "$scratch" >&2; fi
}
trap finish EXIT
cargo metadata --locked --offline --manifest-path "$ROOT/testing/canic-adapter/Cargo.toml" --format-version 1 > "$scratch/workspace.json"
CARGO_TARGET_DIR="$(jq -er '.target_directory' "$scratch/workspace.json")"
export CARGO_TARGET_DIR
facade_path="$(jq -n --arg path "$ROOT/crates/icydb" '$path')"
fixture_path="$(jq -n --arg path "$ROOT/testing/canic-adapter/tests/pass/endpoint_msrv.rs" '$path')"
adapter_version="$(jq -er '.packages[] | select(.name == "canic-icydb") | .version' "$scratch/workspace.json")"
for dependency in icydb runtime_api; do
  consumer="$scratch/$dependency"
  mkdir -p "$consumer/src"
  cat > "$consumer/Cargo.toml" <<TOML
[package]
name = "icydb-endpoint-compiler-probe"
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
canic-icydb = "=$adapter_version"
[workspace]
TOML
  if [[ "$dependency" == icydb ]]; then
    printf 'extern crate icydb as runtime_api;\n' > "$consumer/src/lib.rs"
  fi
  printf 'include!(%s);\n' "$fixture_path" >> "$consumer/src/lib.rs"
  cp "$ROOT/testing/canic-adapter/Cargo.lock" "$consumer/Cargo.lock"
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

