use icydb::design::prelude::*;

///
/// QuickstartCanister
///
/// Test-only canister model used by runtime SQL integration harnesses.
///

#[canister(memory_min = 50, memory_max = 100, commit_memory_id = 100)]
pub struct QuickstartCanister {}

///
/// QuickstartStore
///
/// Shared fixture store for runtime SQL integration entities.
///

#[store(
    ident = "QUICKSTART_STORE",
    canister = "QuickstartCanister",
    data_memory_id = 50,
    index_memory_id = 51
)]
pub struct QuickstartStore {}
