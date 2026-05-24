use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::define_fixture_canister_store;

define_fixture_canister_store!(
    MinimalCanister = "MinimalCanister",
    MinimalStore = "MINIMAL_STORE",
    namespace = "minimal",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102,
);
