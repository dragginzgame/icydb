use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_complex_audit_entities, define_complex_audit_types};

///
/// OneComplexCanister
///
/// Single-entity complex canister model used for wasm-footprint auditing.
///

#[canister(
    memory_namespace = "one_complex",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 103
)]
pub struct OneComplexCanister {}

///
/// OneComplexStore
///
/// Shared store used to measure one repeated complex entity shape.
///

#[store(
    ident = "ONE_COMPLEX_STORE",
    store_name = "main",
    canister = "OneComplexCanister",
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102
)]
pub struct OneComplexStore {}

define_complex_audit_types!();
define_complex_audit_entities!("OneComplexStore", "OneComplexEntity01"; OneComplexEntity01);
