use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{
    define_complex_audit_entities, define_complex_audit_types, define_fixture_canister_store,
};

define_fixture_canister_store!(
    OneComplexCanister = "OneComplexCanister",
    OneComplexStore = "ONE_COMPLEX_STORE",
    namespace = "one_complex",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102,
);

define_complex_audit_types!();
define_complex_audit_entities!("OneComplexStore", "OneComplexEntity01"; OneComplexEntity01);
