use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    DemoRpgCanister = "DemoRpgCanister",
    namespace = "demo_rpg",
    memory_min = 104,
    memory_max = 108,
    commit_memory_id = 108,
);

define_fixture_store!(
    DemoRpgStore = "DEMO_RPG_STORE",
    canister = "DemoRpgCanister",
    storage(journaled(
        data_memory_id = 104,
        index_memory_id = 105,
        schema_memory_id = 106,
        journal_memory_id = 107,
    )),
);
