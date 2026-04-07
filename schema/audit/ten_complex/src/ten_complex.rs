use icydb::design::prelude::*;
use icydb_testing_wasm_helpers::{define_complex_audit_entities, define_complex_audit_types};

///
/// TenComplexCanister
///
/// Ten-entity complex canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 176, memory_max = 206, commit_memory_id = 178)]
pub struct TenComplexCanister {}

///
/// TenComplexStore
///
/// Shared store used to measure ten repeated complex entity shapes.
///

#[store(
    ident = "TEN_COMPLEX_STORE",
    canister = "TenComplexCanister",
    data_memory_id = 176,
    index_memory_id = 177
)]
pub struct TenComplexStore {}

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
