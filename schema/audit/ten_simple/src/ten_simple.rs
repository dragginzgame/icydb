use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::define_simple_audit_entities;

///
/// TenSimpleCanister
///
/// Ten-entity simple canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 155, memory_max = 175, commit_memory_id = 157)]
pub struct TenSimpleCanister {}

///
/// TenSimpleStore
///
/// Shared store used to measure ten repeated simple entity shapes.
///

#[store(
    ident = "TEN_SIMPLE_STORE",
    canister = "TenSimpleCanister",
    data_memory_id = 155,
    index_memory_id = 156,
    schema_memory_id = 158
)]
pub struct TenSimpleStore {}

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
