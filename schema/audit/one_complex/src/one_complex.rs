use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{
    define_complex_audit_entities, define_complex_audit_types, define_fixture_canister,
    define_fixture_store,
};

define_fixture_canister!(
    OneComplexCanister = "OneComplexCanister",
    namespace = "one_complex",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103,
);

define_fixture_store!(
    OneComplexStore = "ONE_COMPLEX_STORE",
    canister = "OneComplexCanister",
    storage(stable(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
    )),
);

define_complex_audit_types!();
define_complex_audit_entities!("OneComplexStore", "OneComplexEntity01"; OneComplexEntity01);
