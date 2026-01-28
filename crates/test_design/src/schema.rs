pub use crate::prelude::*;

///
/// TestCanister
///

#[canister(memory_min = 50, memory_max = 100)]
pub struct TestCanister {}

///
/// TestDataStore
///

#[store(
    ident = "TEST_DATA_STORE",
    ty = "Data",
    canister = "TestCanister",
    memory_id = 50
)]
pub struct TestDataStore {}

///
/// TestIndexStore
///

#[store(
    ident = "TEST_INDEX_STORE",
    ty = "Index",
    canister = "TestCanister",
    memory_id = 51
)]
pub struct TestIndexStore {}
