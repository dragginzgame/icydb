use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{
    define_complex_audit_entities, define_complex_audit_types, define_fixture_canister,
    define_fixture_store,
};

define_fixture_canister!(
    TenComplexCanister = "TenComplexCanister",
    namespace = "ten_complex",
    memory_min = 176,
    memory_max = 206,
    commit_memory_id = 178,
);

define_fixture_store!(
    TenComplexStore = "TEN_COMPLEX_STORE",
    canister = "TenComplexCanister",
    storage(stable(
        data_memory_id = 176,
        index_memory_id = 177,
        schema_memory_id = 179,
    )),
);

define_complex_audit_types!();
define_complex_audit_entities!(
    "TenComplexStore",
    "TenComplexEntity01";
    TenComplexEntity01,
    TenComplexEntity02,
    TenComplexEntity03,
    TenComplexEntity04,
    TenComplexEntity05,
    TenComplexEntity06,
    TenComplexEntity07,
    TenComplexEntity08,
    TenComplexEntity09,
    TenComplexEntity10,
);
