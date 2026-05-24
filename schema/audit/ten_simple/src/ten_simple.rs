use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister_store, define_simple_audit_entities};

define_fixture_canister_store!(
    TenSimpleCanister = "TenSimpleCanister",
    TenSimpleStore = "TEN_SIMPLE_STORE",
    namespace = "ten_simple",
    memory_min = 155,
    memory_max = 175,
    commit_memory_id = 157,
    data_memory_id = 155,
    index_memory_id = 156,
    schema_memory_id = 158,
);

define_simple_audit_entities!(
    "TenSimpleStore";
    TenSimpleEntity01,
    TenSimpleEntity02,
    TenSimpleEntity03,
    TenSimpleEntity04,
    TenSimpleEntity05,
    TenSimpleEntity06,
    TenSimpleEntity07,
    TenSimpleEntity08,
    TenSimpleEntity09,
    TenSimpleEntity10,
);
