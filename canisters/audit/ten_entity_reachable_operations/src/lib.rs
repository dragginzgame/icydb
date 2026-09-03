//!
//! Ten-entity reachable-operation canister used for Wasm specialization auditing.
//!

use icydb::db::{TypedAdapterError, TypedOperationError};
use icydb_testing_audit_ten_simple_fixtures::ten_simple::{
    ReachableInputChoice, ReachableInputProfile, TenSimpleEntity01, TenSimpleEntity01Insert,
    TenSimpleEntity01Patch, TenSimpleEntity02, TenSimpleEntity02Insert, TenSimpleEntity02Patch,
    TenSimpleEntity03, TenSimpleEntity03Insert, TenSimpleEntity03Patch, TenSimpleEntity04,
    TenSimpleEntity04Insert, TenSimpleEntity04Patch, TenSimpleEntity05, TenSimpleEntity05Insert,
    TenSimpleEntity05Patch, TenSimpleEntity06, TenSimpleEntity06Insert, TenSimpleEntity06Patch,
    TenSimpleEntity07, TenSimpleEntity07Insert, TenSimpleEntity07Patch, TenSimpleEntity08,
    TenSimpleEntity08Insert, TenSimpleEntity08Patch, TenSimpleEntity09, TenSimpleEntity09Insert,
    TenSimpleEntity09Patch, TenSimpleEntity10, TenSimpleEntity10Insert, TenSimpleEntity10Patch,
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
    let Ok(binding) = TenSimpleEntity01::typed_binding(database) else {
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

macro_rules! dispatch_entity {
    ($database:expr, $operation:expr, $entity:expr, $structural_input:expr, $(($slot:literal, $row:ty, $insert:ident, $patch:ident, $omitted_fields:tt, $structural_field:expr)),+ $(,)?) => {
        match $entity {
            $(
                $slot => execute_simple_reachable_entity_operation!(
                    $database,
                    $operation,
                    $row,
                    $insert,
                    $patch,
                    $omitted_fields,
                    $structural_field,
                    $structural_input,
                ),
            )+
            _ => 0,
        }
    };
}

fn exercise_reachable_entity_operation(entity: u8, operation: u8) -> ((u32, u64),) {
    icydb::db::with_request_execution(|| {
        let start = ic_cdk::api::performance_counter(1);
        let result = match (db(), entity, operation) {
            (Ok(database), 0, 7) => u32::from(unbound_structural_input_rejects(&database)),
            (Ok(database), _, _) => dispatch_entity!(
                &database,
                operation,
                entity,
                reachable_structural_input(operation),
                (
                    0,
                    TenSimpleEntity01,
                    TenSimpleEntity01Insert,
                    TenSimpleEntity01Patch,
                    [profiles = icydb::db::WriteCell::Value(Vec::new())],
                    Some(TenSimpleEntity01::PROFILES.as_str())
                ),
                (
                    1,
                    TenSimpleEntity02,
                    TenSimpleEntity02Insert,
                    TenSimpleEntity02Patch,
                    [],
                    None
                ),
                (
                    2,
                    TenSimpleEntity03,
                    TenSimpleEntity03Insert,
                    TenSimpleEntity03Patch,
                    [],
                    None
                ),
                (
                    3,
                    TenSimpleEntity04,
                    TenSimpleEntity04Insert,
                    TenSimpleEntity04Patch,
                    [],
                    None
                ),
                (
                    4,
                    TenSimpleEntity05,
                    TenSimpleEntity05Insert,
                    TenSimpleEntity05Patch,
                    [],
                    None
                ),
                (
                    5,
                    TenSimpleEntity06,
                    TenSimpleEntity06Insert,
                    TenSimpleEntity06Patch,
                    [],
                    None
                ),
                (
                    6,
                    TenSimpleEntity07,
                    TenSimpleEntity07Insert,
                    TenSimpleEntity07Patch,
                    [],
                    None
                ),
                (
                    7,
                    TenSimpleEntity08,
                    TenSimpleEntity08Insert,
                    TenSimpleEntity08Patch,
                    [],
                    None
                ),
                (
                    8,
                    TenSimpleEntity09,
                    TenSimpleEntity09Insert,
                    TenSimpleEntity09Patch,
                    [],
                    None
                ),
                (
                    9,
                    TenSimpleEntity10,
                    TenSimpleEntity10Insert,
                    TenSimpleEntity10Patch,
                    [],
                    None
                ),
            ),
            (Err(_), _, _) => 0,
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
