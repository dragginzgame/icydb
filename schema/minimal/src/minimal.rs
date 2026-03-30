use icydb::design::prelude::*;

///
/// MinimalCanister
///
/// Minimal canister model used for wasm-footprint SQL auditing.
///

#[canister(memory_min = 61, memory_max = 71, commit_memory_id = 63)]
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
    data_memory_id = 61,
    index_memory_id = 62
)]
pub struct MinimalStore {}
