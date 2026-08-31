//!
//! Ten-entity reachable-operation canister used for Wasm specialization auditing.
//!

use icydb_testing_audit_ten_simple_fixtures::ten_simple::{
    TenSimpleEntity01, TenSimpleEntity01Insert, TenSimpleEntity01Patch, TenSimpleEntity02,
    TenSimpleEntity02Insert, TenSimpleEntity02Patch, TenSimpleEntity03, TenSimpleEntity03Insert,
    TenSimpleEntity03Patch, TenSimpleEntity04, TenSimpleEntity04Insert, TenSimpleEntity04Patch,
    TenSimpleEntity05, TenSimpleEntity05Insert, TenSimpleEntity05Patch, TenSimpleEntity06,
    TenSimpleEntity06Insert, TenSimpleEntity06Patch, TenSimpleEntity07, TenSimpleEntity07Insert,
    TenSimpleEntity07Patch, TenSimpleEntity08, TenSimpleEntity08Insert, TenSimpleEntity08Patch,
    TenSimpleEntity09, TenSimpleEntity09Insert, TenSimpleEntity09Patch, TenSimpleEntity10,
    TenSimpleEntity10Insert, TenSimpleEntity10Patch,
};
use icydb_testing_wasm_helpers::execute_simple_reachable_entity_operation;

icydb::start!();

macro_rules! dispatch_entity {
    ($database:expr, $operation:expr, $entity:expr, $(($slot:literal, $row:ty, $insert:ident, $patch:ident)),+ $(,)?) => {
        match $entity {
            $(
                $slot => execute_simple_reachable_entity_operation!(
                    $database,
                    $operation,
                    $row,
                    $insert,
                    $patch,
                ),
            )+
            _ => 0,
        }
    };
}

fn exercise_reachable_entity_operation(entity: u8, operation: u8) -> ((u32, u64),) {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = match db() {
            Ok(database) => dispatch_entity!(
                &database,
                operation,
                entity,
                (
                    0,
                    TenSimpleEntity01,
                    TenSimpleEntity01Insert,
                    TenSimpleEntity01Patch
                ),
                (
                    1,
                    TenSimpleEntity02,
                    TenSimpleEntity02Insert,
                    TenSimpleEntity02Patch
                ),
                (
                    2,
                    TenSimpleEntity03,
                    TenSimpleEntity03Insert,
                    TenSimpleEntity03Patch
                ),
                (
                    3,
                    TenSimpleEntity04,
                    TenSimpleEntity04Insert,
                    TenSimpleEntity04Patch
                ),
                (
                    4,
                    TenSimpleEntity05,
                    TenSimpleEntity05Insert,
                    TenSimpleEntity05Patch
                ),
                (
                    5,
                    TenSimpleEntity06,
                    TenSimpleEntity06Insert,
                    TenSimpleEntity06Patch
                ),
                (
                    6,
                    TenSimpleEntity07,
                    TenSimpleEntity07Insert,
                    TenSimpleEntity07Patch
                ),
                (
                    7,
                    TenSimpleEntity08,
                    TenSimpleEntity08Insert,
                    TenSimpleEntity08Patch
                ),
                (
                    8,
                    TenSimpleEntity09,
                    TenSimpleEntity09Insert,
                    TenSimpleEntity09Patch
                ),
                (
                    9,
                    TenSimpleEntity10,
                    TenSimpleEntity10Insert,
                    TenSimpleEntity10Patch
                ),
            ),
            Err(_) => 0,
        };
        let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        ((result, instructions),)
    })
}

#[ic_cdk::query]
fn exercise_reachable_entity_read(entity: u8, operation: u8) -> ((u32, u64),) {
    if operation > 1 {
        return ((0, 0),);
    }
    exercise_reachable_entity_operation(entity, operation)
}

#[ic_cdk::update]
fn exercise_reachable_entity_write(entity: u8, operation: u8) -> ((u32, u64),) {
    if !(2..=5).contains(&operation) {
        return ((0, 0),);
    }
    exercise_reachable_entity_operation(entity, operation)
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
