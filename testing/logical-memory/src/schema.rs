//! The second build omits one store; the surviving database identity is unchanged.

use icydb::model::prelude::*;

#[canister(memory_namespace = "logical_fixture", memory_profile = "compact")]
pub struct MemoryCanister;

#[store(canister = "MemoryCanister", storage(journaled(key = "keep")))]
pub struct KeepStore;

#[cfg(not(feature = "omit-retiring-store"))]
#[store(canister = "MemoryCanister", storage(journaled(key = "retiring")))]
pub struct RetiringStore;

#[entity(
    store = "KeepStore",
    version = 1,
    pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))))
)]
pub struct KeepRow;

#[cfg(not(feature = "omit-retiring-store"))]
#[entity(
    store = "RetiringStore",
    version = 1,
    pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))))
)]
pub struct RetiringRow;
