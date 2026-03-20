use icydb::design::prelude::*;
use icydb_testing_wasm_fixtures::define_simple_audit_entities;

///
/// OneSimpleCanister
///
/// Single-entity simple canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 30, memory_max = 40, commit_memory_id = 32)]
pub struct OneSimpleCanister {}

///
/// OneSimpleStore
///
/// Shared store used to measure one repeated simple entity shape.
///

#[store(
    ident = "ONE_SIMPLE_STORE",
    canister = "OneSimpleCanister",
    data_memory_id = 30,
    index_memory_id = 31
)]
pub struct OneSimpleStore {}

define_simple_audit_entities!("OneSimpleStore"; OneSimpleEntity01);
