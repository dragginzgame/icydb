use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::define_simple_audit_entities;

///
/// OneSimpleCanister
///
/// Single-entity simple canister model used for wasm-footprint auditing.
///

#[canister(
    db_name = "one_simple",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103
)]
pub struct OneSimpleCanister {}

///
/// OneSimpleStore
///
/// Shared store used to measure one repeated simple entity shape.
///

#[store(
    ident = "ONE_SIMPLE_STORE",
    store_name = "main",
    canister = "OneSimpleCanister",
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102
)]
pub struct OneSimpleStore {}

define_simple_audit_entities!("OneSimpleStore"; OneSimpleEntity01);
