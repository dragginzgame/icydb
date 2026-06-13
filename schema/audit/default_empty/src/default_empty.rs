use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    DefaultEmptyCanister = "DefaultEmptyCanister",
    namespace = "default_empty",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
);

define_fixture_store!(
    DefaultEmptyStore = "DEFAULT_EMPTY_STORE",
    canister = "DefaultEmptyCanister",
    storage(journaled(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
        journal_memory_id = 104,
    )),
);
