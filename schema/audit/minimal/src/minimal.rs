use icydb::design::prelude::*;

///
/// MinimalCanister
///
/// Minimal canister model used for wasm-footprint SQL auditing.
///

#[canister(
    memory_namespace = "minimal",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103
)]
pub struct MinimalCanister {}

///
/// MinimalStore
///
/// Empty store-only model used to measure the bare database surface with no
/// entities registered.
///

#[store(
    ident = "MINIMAL_STORE",
    store_name = "main",
    canister = "MinimalCanister",
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102
)]
pub struct MinimalStore {}
