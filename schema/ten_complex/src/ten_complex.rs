use icydb::design::prelude::*;
use icydb_testing_wasm_fixtures::{define_complex_audit_entities, define_complex_audit_types};

///
/// TenComplexCanister
///
/// Ten-entity complex canister model used for wasm-footprint auditing.
///

#[canister(memory_min = 60, memory_max = 90, commit_memory_id = 62)]
pub struct TenComplexCanister {}

///
/// TenComplexStore
///
/// Shared store used to measure ten repeated complex entity shapes.
///

#[store(
    ident = "TEN_COMPLEX_STORE",
    canister = "TenComplexCanister",
    data_memory_id = 60,
    index_memory_id = 61
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
