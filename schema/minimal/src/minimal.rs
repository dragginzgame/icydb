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
/// Single-store model for the minimal queryable entity surface.
///

#[store(
    ident = "MINIMAL_STORE",
    canister = "MinimalCanister",
    data_memory_id = 20,
    index_memory_id = 21
)]
pub struct MinimalStore {}

///
/// MinimalEntity
///
/// Single-entity schema used to wire SQL query flow without fixture payloads.
///

#[entity(
    store = "MinimalStore",
    pk(field = "id"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "name", value(item(prim = "Text")), default = "String::new")
    )
)]
pub struct MinimalEntity {}
