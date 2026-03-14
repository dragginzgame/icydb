use icydb::design::prelude::*;

///
/// SqlTestCanister
///
/// Test-only canister model used by runtime SQL integration harnesses.
///

#[canister(memory_min = 50, memory_max = 100, commit_memory_id = 100)]
pub struct SqlTestCanister {}

///
/// SqlTestStore
///
/// Shared fixture store for runtime SQL integration entities.
///

#[store(
    ident = "SQL_TEST_STORE",
    canister = "SqlTestCanister",
    data_memory_id = 50,
    index_memory_id = 51
)]
pub struct SqlTestStore {}
