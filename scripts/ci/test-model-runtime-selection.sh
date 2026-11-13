#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh
source "$ROOT/vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh"
mkdir -p "$ROOT/target"
FIXTURE="$(fixture_create "$ROOT/target/model-runtime-selection.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$FIXTURE" "Model runtime selection fixture retained"' EXIT
mkdir -p "$FIXTURE/src"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

manifest() {
  cat > "$FIXTURE/Cargo.toml" <<TOML
[package]
name = "model-runtime-selection"
version = "0.0.0"
edition = "2024"
[workspace]
TOML
  cp "$ROOT/Cargo.lock" "$FIXTURE/Cargo.lock"
}
model_dependency() {
  printf 'model_api = { package = "icydb-model", path = "%s/crates/icydb-model" }\n' "$ROOT"
}
runtime_dependency() {
  printf 'runtime_api = { package = "icydb", path = "%s/crates/icydb", default-features = false%s }\n' "$ROOT" "${1:-}"
}
check() {
  local name="$1"
  shift
  if ! cargo check --manifest-path "$FIXTURE/Cargo.toml" --lib --offline "$@" > "$FIXTURE/$name.log" 2>&1; then
    cat "$FIXTURE/$name.log" >&2
    return 1
  fi
  printf '[OK] model runtime selection: %s\n' "$name"
}

# Runtime presence must never change direct-model output, even in a build that
# can resolve the runtime. Conflicting inherent names detect accidental emission.
cat > "$FIXTURE/model.rs" <<'RUST'
use model_api::prelude::*;
#[canister(memory_namespace = "selection")]
pub struct SelectionCanister {}
#[store(canister = "SelectionCanister", storage(heap()))]
pub struct SelectionStore {}
#[entity(store = "SelectionStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64")))))]
pub struct SelectionEntity {}
#[record(fields(field(name = "value", value(item(prim = "Nat64")))))]
pub struct SelectionRecord {}
impl SelectionEntity { pub const ID: u8 = 7; }
impl SelectionRecord { pub const VALUE: u8 = 9; }
RUST
cp "$FIXTURE/model.rs" "$FIXTURE/src/lib.rs"
for selection in absent dev inactive optional normal build; do
  manifest
  {
    printf '[dependencies]\n'
    model_dependency
    case "$selection" in
      absent) ;;
      dev) printf '[dev-dependencies]\n'; runtime_dependency ;;
      inactive) printf "[target.'cfg(target_arch = \"wasm32\")'.dependencies]\n"; runtime_dependency ;;
      optional) runtime_dependency ', optional = true' ;;
      normal) runtime_dependency ;;
      build) printf '[build-dependencies]\n'; runtime_dependency ;;
    esac
  } >> "$FIXTURE/Cargo.toml"
  check "model-$selection"
done

# The facade must supply both typed entity adapters and record field references
# with renamed dependencies, including when Cargo enables an optional/target dep.
cat > "$FIXTURE/runtime.rs" <<'RUST'
use runtime_api::model::prelude::*;
#[canister(memory_namespace = "selection")]
pub struct SelectionCanister {}
#[store(canister = "SelectionCanister", storage(heap()))]
pub struct SelectionStore {}
#[entity(store = "SelectionStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64")))))]
pub struct SelectionEntity {}
#[runtime_api::model::record(fields(field(name = "value", value(item(prim = "Nat64")))))]
pub struct SelectionRecord {}
const _: runtime_api::db::query::FieldRef = SelectionEntity::ID;
const _: runtime_api::db::query::FieldRef = SelectionRecord::VALUE;
fn assert_adapter<T: runtime_api::db::TypedWriteAdapter>() {}
fn assert_row<T: runtime_api::db::TypedRowAdapter>() {}
pub fn qualify() {
    assert_adapter::<SelectionEntityInsert>();
    assert_adapter::<SelectionEntityPatch>();
    assert_adapter::<SelectionEntityReplace>();
    assert_row::<SelectionEntity>();
}
RUST
for selection in facade both active optional; do
  manifest
  cp "$FIXTURE/runtime.rs" "$FIXTURE/src/lib.rs"
  {
    printf '[dependencies]\n'
    if [[ "$selection" == both ]]; then
      model_dependency
    fi
    if [[ "$selection" == active ]]; then
      printf "[target.'cfg(not(target_arch = \"wasm32\"))'.dependencies]\n"
    fi
    if [[ "$selection" == optional ]]; then
      runtime_dependency ', optional = true'
    else
      runtime_dependency
    fi
  } >> "$FIXTURE/Cargo.toml"
  if [[ "$selection" == both ]]; then
    {
      cat <<'RUST'
pub use model_api as bridge;
#[runtime_api::model::record(model_crate = "crate::bridge",
    fields(field(name = "other", value(item(prim = "Nat64")))))]
pub struct OverrideRecord {}
const _: runtime_api::db::query::FieldRef = OverrideRecord::OTHER;
RUST
      # Direct model declarations remain schema-only alongside facade declarations.
      printf 'pub mod schema_only {\n'
      cat "$FIXTURE/model.rs"
      printf '}\n'
    } >> "$FIXTURE/src/lib.rs"
  fi
  if [[ "$selection" == optional ]]; then
    check "runtime-$selection" --features runtime_api
  else
    check "runtime-$selection"
  fi
  if [[ "$selection" == facade ]]; then
    check runtime-facade-wasm --target wasm32-unknown-unknown
  fi
done
fixture_complete=true
