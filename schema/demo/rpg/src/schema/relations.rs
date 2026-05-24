use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::define_fixture_canister_store;

define_fixture_canister_store!(
    DemoRpgCanister = "DemoRpgCanister",
    DemoRpgStore = "DEMO_RPG_STORE",
    namespace = "demo_rpg",
    memory_min = 104,
    memory_max = 154,
    commit_memory_id = 154,
    data_memory_id = 104,
    index_memory_id = 105,
    schema_memory_id = 106,
);
