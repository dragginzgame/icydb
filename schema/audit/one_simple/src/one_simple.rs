use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{
    define_fixture_canister, define_fixture_store, define_simple_audit_entities,
};

define_fixture_canister!(
    OneSimpleCanister = "OneSimpleCanister",
    namespace = "one_simple",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
);

define_fixture_store!(
    OneSimpleStore = "ONE_SIMPLE_STORE",
    canister = "OneSimpleCanister",
    storage(stable(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
    )),
);

define_simple_audit_entities!("OneSimpleStore"; OneSimpleEntity01);
