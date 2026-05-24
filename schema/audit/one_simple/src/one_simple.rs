use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister_store, define_simple_audit_entities};

define_fixture_canister_store!(
    OneSimpleCanister = "OneSimpleCanister",
    OneSimpleStore = "ONE_SIMPLE_STORE",
    namespace = "one_simple",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102,
);

define_simple_audit_entities!("OneSimpleStore"; OneSimpleEntity01);
