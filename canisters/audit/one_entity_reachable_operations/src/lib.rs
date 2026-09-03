//!
//! One-entity reachable-operation canister used for Wasm specialization auditing.
//!

use icydb::db::{TypedAdapterError, TypedOperationError};
use icydb_testing_audit_one_simple_fixtures::one_simple::{
    OneSimpleEntity01, OneSimpleEntity01Insert, OneSimpleEntity01Patch, ReachableInputChoice,
    ReachableInputProfile,
};
use icydb_testing_wasm_helpers::execute_simple_reachable_entity_operation;

icydb::start!();

#[icydb_model::record(fields(field(name = "label", value(item(prim = "Text", max_len = 64)))))]
pub struct UnboundReachableInput {}

fn reachable_structural_input(operation: u8) -> Option<Vec<ReachableInputProfile>> {
    (operation == 6).then(|| {
        vec![
            ReachableInputProfile {
                label: "Ada".to_string(),
                choice: ReachableInputChoice::Ready,
                note: None,
            },
            ReachableInputProfile {
                label: "Grace".to_string(),
                choice: ReachableInputChoice::Weighted(7),
                note: Some("nested enum payload".to_string()),
            },
        ]
    })
}

fn unbound_structural_input_rejects<C: icydb::traits::CanisterKind>(
    database: &icydb::db::DbSession<C>,
) -> bool {
    let Ok(binding) = OneSimpleEntity01::typed_binding(database) else {
        return false;
    };
    matches!(
        database.bind_typed_input(
            &binding,
            UnboundReachableInput {
                label: "unbound".to_string(),
            },
        ),
        Err(TypedOperationError::Adapter(
            TypedAdapterError::FieldUnavailable
        ))
    )
}

fn exercise_reachable_entity_operation(entity: u8, operation: u8) -> ((u32, u64),) {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = match (db(), entity, operation) {
            (Ok(database), 0, 7) => u32::from(unbound_structural_input_rejects(&database)),
            (Ok(database), 0, _) => execute_simple_reachable_entity_operation!(
                &database,
                operation,
                OneSimpleEntity01,
                OneSimpleEntity01Insert,
                OneSimpleEntity01Patch,
                [profiles = icydb::db::WriteCell::Value(Vec::new())],
                Some(OneSimpleEntity01::PROFILES.as_str()),
                reachable_structural_input(operation),
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
    if !(2..=7).contains(&operation) {
        return ((0, 0),);
    }
    exercise_reachable_entity_operation(entity, operation)
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
