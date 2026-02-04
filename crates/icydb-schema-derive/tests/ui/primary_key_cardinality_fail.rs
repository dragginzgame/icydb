use icydb_schema_derive::{canister, data_store, entity};

#[canister(memory_min = 1, memory_max = 2)]
pub struct TestCanister;

#[data_store(ident = "TEST_DATA_STORE", canister = "TestCanister", memory_id = 0)]
pub struct TestDataStore;

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(opt, item(prim = "Ulid"))))
)]
/// Compile-fail fixture for primary key cardinality.
/// Intentionally marks the primary key as optional.
/// Expected to fail with a fatal schema error.

pub struct BadPrimaryKeyCardinality;

fn main() {}
