use icydb::design::prelude::*;
use icydb_testing_wasm_fixtures::{define_complex_audit_entities, define_complex_audit_types};

///
/// OneComplexCanister
///
/// Single-entity complex canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 83, memory_max = 103, commit_memory_id = 85)]
pub struct OneComplexCanister {}

///
/// OneComplexStore
///
/// Shared store used to measure one repeated complex entity shape.
///

#[store(
    ident = "ONE_COMPLEX_STORE",
    canister = "OneComplexCanister",
    data_memory_id = 83,
    index_memory_id = 84
)]
pub struct OneComplexStore {}

define_complex_audit_types!();
define_complex_audit_entities!("OneComplexStore", "OneComplexEntity01"; OneComplexEntity01);
