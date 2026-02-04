use icydb_schema_derive::{canister, data_store, entity};
use icydb::design::prelude::*;

#[canister(memory_min = 1, memory_max = 2)]
pub struct TestCanister;

#[data_store(ident = "TEST_DATA_STORE", canister = "TestCanister", memory_id = 0)]
pub struct TestDataStore;

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Ulid"))))
)]
pub struct Owner;

#[entity(
    store = "TestDataStore",
    pk = "owner",
    fields(field(ident = "owner", value(item(rel = "Owner"))))
)]
/// Compile-fail fixture for relation primary keys.
/// Intentionally uses a relation as the primary key.
/// Expected to fail with a fatal schema error.

pub struct BadOwner;

fn main() {}
