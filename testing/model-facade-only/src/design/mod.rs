//! Shared-source declarations for host generation and the canister runtime.

use icydb::model::prelude::*;
use runtime_api as icydb;

#[canister(memory_namespace = "facade_only")]
pub struct FacadeCanister {}

#[store(canister = "FacadeCanister", storage(journaled(key = "main")))]
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
