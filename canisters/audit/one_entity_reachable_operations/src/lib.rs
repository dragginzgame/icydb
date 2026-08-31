//!
//! One-entity reachable-operation canister used for Wasm specialization auditing.
//!

use icydb_testing_audit_one_simple_fixtures::one_simple::{
    OneSimpleEntity01, OneSimpleEntity01Insert, OneSimpleEntity01Patch,
};
use icydb_testing_wasm_helpers::execute_simple_reachable_entity_operation;

icydb::start!();

fn exercise_reachable_entity_operation(entity: u8, operation: u8) -> ((u32, u64),) {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = match (db(), entity) {
            (Ok(database), 0) => execute_simple_reachable_entity_operation!(
                &database,
                operation,
                OneSimpleEntity01,
                OneSimpleEntity01Insert,
                OneSimpleEntity01Patch,
            ),
            _ => 0,
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
