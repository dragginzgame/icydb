use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{
    define_fixture_canister, define_fixture_store, define_simple_audit_entities,
};

define_fixture_canister!(
    TenSimpleCanister = "TenSimpleCanister",
    namespace = "ten_simple",
    memory_min = 155,
    memory_max = 160,
    commit_memory_id = 159,
);

define_fixture_store!(
    TenSimpleStore = "TEN_SIMPLE_STORE",
    canister = "TenSimpleCanister",
    storage(journaled(
        data_memory_id = 155,
        index_memory_id = 156,
        schema_memory_id = 157,
        journal_memory_id = 158,
    )),
);

define_simple_audit_entities!(
    "TenSimpleStore";
    TenSimpleEntity01 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity01",
    TenSimpleEntity02 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity02",
    TenSimpleEntity03 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity03",
    TenSimpleEntity04 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity04",
    TenSimpleEntity05 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity05",
    TenSimpleEntity06 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity06",
    TenSimpleEntity07 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity07",
    TenSimpleEntity08 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity08",
    TenSimpleEntity09 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity09",
    TenSimpleEntity10 => "schema/audit/ten_simple/src/ten_simple.rs::TenSimpleEntity10",
);
