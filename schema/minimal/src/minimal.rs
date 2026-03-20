use icydb::design::prelude::*;

///
/// MinimalCanister
///
/// Minimal canister model used for wasm-footprint SQL auditing.
///

#[canister(memory_min = 20, memory_max = 30, commit_memory_id = 22)]
pub struct MinimalCanister {}

///
/// MinimalStore
///
/// Empty store-only model used to measure the bare database surface with no
/// entities registered.
///

#[store(
    ident = "MINIMAL_STORE",
    canister = "MinimalCanister",
    data_memory_id = 20,
    index_memory_id = 21
)]
pub struct MinimalStore {}
