//! Fixed structural writes and generated typed reads for added named values.

use crate::{typed_fixture_invariant_error, typed_operation_fixture_error};

use icydb::{
    db::{StructuralPatch, WriteCell, with_request_execution},
    traits::EntitySource,
    types::Id,
    value::InputValue,
};
use icydb_testing_test_sql_fixtures::entity_creation::{Quest, QuestState};
#[cfg(feature = "entity-rename-successor")]
use icydb_testing_test_sql_fixtures::entity_rename::CatalogItem as RelationTarget;
#[cfg(not(feature = "entity-rename-successor"))]
use icydb_testing_test_sql_fixtures::entity_rename::Item as RelationTarget;

/// Local instruction interval around one complete fixture request.

#[derive(serde::Deserialize, serde::Serialize)]
pub struct EntityCreationMeasurement {
    result: Result<(), icydb::Error>,
    local_instructions: u64,
}

/// Exercise the new entity's reverse relation through a fixed trusted delete.

fn check_created_quest_target_delete() -> Result<icydb::Error, icydb::Error> {
    with_request_execution(|| {
        let sql = format!("DELETE FROM {} WHERE id = 3", RelationTarget::ENTITY);
        match icydb::db!()?.execute_trusted_sql_mutation(&sql) {
            Err(error) => Ok(error),
            Ok(_) => Err(typed_fixture_invariant_error()),
        }
    })
}
icydb_testing_wasm_helpers::export_wire!(update, "check_created_quest_target_delete", CheckCreatedQuestTargetDelete, check_created_quest_target_delete, (), Result<icydb::Error, icydb::Error>);

/// Insert a new row through accepted structural authority, including named values.

fn write_created_quest(id: u64, item_id: u64, code: u64) -> EntityCreationMeasurement {
    let start = crate::call_context_instruction_counter();
    let result = with_request_execution(|| {
        let session = icydb::db!()?;
        let patch = StructuralPatch::new()
            .field("id", WriteCell::Value(InputValue::nat64(id)))
            .field("item_id", WriteCell::Value(InputValue::nat64(item_id)))
            .field("code", WriteCell::Value(InputValue::nat64(code)))
            .field("state", WriteCell::Value(InputValue::loose_enum("Active")))
            .field(
                "details",
                WriteCell::Value(InputValue::map(vec![
                    (
                        InputValue::text("label".into()),
                        InputValue::text("quest".into()),
                    ),
                    (InputValue::text("reward".into()), InputValue::nat64(42)),
                ])),
            );
        session.execute_trusted_structural_insert_batch(Quest::ENTITY, vec![patch])?;
        Ok(())
    });
    EntityCreationMeasurement {
        result,
        local_instructions: crate::call_context_instruction_counter().saturating_sub(start),
    }
}
icydb_testing_wasm_helpers::export_wire!(update, "write_created_quest", WriteCreatedQuest, write_created_quest, (id: u64, item_id: u64, code: u64), EntityCreationMeasurement);

/// Decode the added record and enum through the generated entity's accepted binding.

fn check_created_quest() -> EntityCreationMeasurement {
    let start = crate::call_context_instruction_counter();
    let result = with_request_execution(|| {
        let row = icydb::db!()?
            .get::<Quest>(Id::from_key(7))
            .map_err(typed_operation_fixture_error)?
            .ok_or_else(typed_fixture_invariant_error)?;
        if (row.id, row.item_id, row.code) != (7, 3, 700)
            || row.state != QuestState::Active
            || row.details.label != "quest"
            || row.details.reward != 42
        {
            return Err(typed_fixture_invariant_error());
        }
        Ok(())
    });
    EntityCreationMeasurement {
        result,
        local_instructions: crate::call_context_instruction_counter().saturating_sub(start),
    }
}
icydb_testing_wasm_helpers::export_wire!(
    query,
    "check_created_quest",
    CheckCreatedQuest,
    check_created_quest,
    (),
    EntityCreationMeasurement
);
