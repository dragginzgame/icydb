use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::define_simple_audit_entities;

///
/// OneSimpleCanister
///
/// Single-entity simple canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 72, memory_max = 82, commit_memory_id = 74)]
pub struct OneSimpleCanister {}

///
/// OneSimpleStore
///
/// Shared store used to measure one repeated simple entity shape.
///

#[store(
    ident = "ONE_SIMPLE_STORE",
    canister = "OneSimpleCanister",
    data_memory_id = 72,
    index_memory_id = 73,
    schema_memory_id = 75
)]
pub struct OneSimpleStore {}

define_simple_audit_entities!("OneSimpleStore"; OneSimpleEntity01);
