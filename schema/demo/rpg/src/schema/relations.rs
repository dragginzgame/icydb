use icydb::design::prelude::*;

///
/// DemoRpgCanister
///
/// Test-only canister model used by runtime SQL integration harnesses.
///

#[canister(memory_min = 104, memory_max = 154, commit_memory_id = 154)]
pub struct DemoRpgCanister {}

///
/// DemoRpgStore
///
/// Shared fixture store for runtime SQL integration entities.
///

#[store(
    ident = "DEMO_RPG_STORE",
    canister = "DemoRpgCanister",
    data_memory_id = 104,
    index_memory_id = 105
)]
pub struct DemoRpgStore {}
