//! Shared-source declarations for host generation and the canister runtime.

use icydb::model::prelude::*;
use runtime_api as icydb;

#[canister(
    memory_namespace = "facade_only",
    memory_min = 220,
    memory_max = 226,
    commit_memory_id = 224,
    startup_memory_id = 225,
    integrity_progress_memory_id = 226
)]
pub struct FacadeCanister {}

#[store(
    canister = "FacadeCanister",
    storage(journaled(
        data_memory_id = 220,
        index_memory_id = 221,
        schema_memory_id = 222,
        journal_memory_id = 223
    ))
)]
pub struct FacadeStore {}

#[record(
    traits(add(Serialize)),
    fields(
        field(name = "rank", value(item(prim = "Nat64"))),
        field(name = "label", value(item(prim = "Text", max_len = 64)))
    )
)]
pub struct FacadeProfile {}

#[entity(
    store = "FacadeStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "profile", value(item(is = "FacadeProfile")))
    )
)]
pub struct FacadePlayer {}
