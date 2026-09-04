//! Module: index::plan
//! Responsibility: preflight planning for deterministic index mutations.
//! Does not own: commit marker protocol or runtime apply sequencing.
//! Boundary: executor/commit call this module before writing commit markers.

mod delta;
mod error;
mod integrity;
mod read;
mod unique;

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::{CanonicalSlotReader, StructuralRowContract},
        index::{IndexKey, IndexReadContract, IndexRowIdentity},
        key_taxonomy::PrimaryKeyValue,
        predicate::{PredicateProgram, normalized_accepted_index_predicate},
        schema::{
            SchemaExpressionIndexInfo, SchemaExpressionIndexKeyItemInfo, SchemaIndexInfo,
            SchemaInfo,
        },
    },
    error::{InternalError, MutationDiagnosticContext},
    types::EntityTag,
};
use error::IndexPlanError;

pub(in crate::db) use delta::{
    IndexDelta, IndexDeltaGroup, IndexMembershipDelta, IndexMutationPlan,
};
pub(in crate::db) use integrity::{AcceptedIndexInspectionDomain, AcceptedIndexInspectionPlan};
pub(in crate::db) use read::IndexPlanReadView;

// Distinguish the two structural key-build lanes so planner diagnostics can
// preserve the existing insertion-vs-removal error taxonomy.
#[derive(Clone, Copy)]
enum IndexKeyLane {
    Old,
    New,
}

impl IndexKeyLane {
    // Map one missing entity-key case back onto the planner-owned internal error.
    fn missing_entity_key_error(self) -> InternalError {
        match self {
            Self::Old => InternalError::structural_index_removal_entity_key_required(),
            Self::New => InternalError::structural_index_insertion_entity_key_required(),
        }
    }
}

// Compile one accepted mutation-lane index predicate. Malformed persisted SQL
// remains fail-closed as `False`; integrity inspection owns corruption errors.
fn accepted_index_mutation_predicate_program(
    predicate_sql: Option<&str>,
    row_contract: &StructuralRowContract,
) -> Option<PredicateProgram> {
    normalized_accepted_index_predicate(predicate_sql)
        .map(|predicate| PredicateProgram::compile_with_row_contract(row_contract, &predicate))
}

pub(in crate::db::index::plan) fn accepted_field_path_index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    accepted_index: &SchemaIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: &PrimaryKeyValue,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    if let Some(predicate_program) = predicate_program {
        let keep_row = predicate_program.eval_with_structural_slot_reader(slots)?;
        if !keep_row {
            return Ok(None);
        }
    }

    IndexKey::new_from_slots_with_accepted_field_path_index_primary_key_value(
        entity_tag,
        primary_key,
        accepted_index,
        slots,
    )
}

pub(in crate::db::index::plan) fn accepted_expression_index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    accepted_index: &SchemaExpressionIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: &PrimaryKeyValue,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    if let Some(predicate_program) = predicate_program {
        let keep_row = predicate_program.eval_with_structural_slot_reader(slots)?;
        if !keep_row {
            return Ok(None);
        }
    }

    IndexKey::new_from_slots_with_accepted_expression_index_primary_key_value(
        entity_tag,
        primary_key,
        accepted_index,
        slots,
    )
}

fn load_structural_accepted_field_path_index_key(
    lane: IndexKeyLane,
    entity_tag: EntityTag,
    accepted_index: &SchemaIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: Option<&PrimaryKeyValue>,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    let Some(primary_key) = primary_key else {
        return Err(lane.missing_entity_key_error());
    };

    accepted_field_path_index_key_for_slot_reader_with_membership_structural(
        entity_tag,
        accepted_index,
        predicate_program,
        primary_key,
        slots,
    )
}

fn load_structural_accepted_expression_index_key(
    lane: IndexKeyLane,
    entity_tag: EntityTag,
    accepted_index: &SchemaExpressionIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: Option<&PrimaryKeyValue>,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    let Some(primary_key) = primary_key else {
        return Err(lane.missing_entity_key_error());
    };

    accepted_expression_index_key_for_slot_reader_with_membership_structural(
        entity_tag,
        accepted_index,
        predicate_program,
        primary_key,
        slots,
    )
}

// Prove that the pre-existing old index entry still contains the expected row
// membership before commit planning becomes purely mechanical.
fn validate_existing_old_index_membership(
    old_primary_key: Option<&PrimaryKeyValue>,
    old_key: Option<&IndexKey>,
    old_entry: Option<&IndexRowIdentity>,
) -> Result<(), InternalError> {
    if old_key.is_none() {
        return Ok(());
    }

    let Some(old_primary_key) = old_primary_key else {
        return Err(InternalError::structural_index_removal_entity_key_required());
    };

    let entry = old_entry
        .as_ref()
        .ok_or_else(InternalError::structural_index_entry_corruption)?;

    if !entry.contains(old_primary_key) {
        return Err(InternalError::structural_index_entry_corruption());
    }

    Ok(())
}

/// Plan all index mutations for one persisted-row transition using structural
/// entity authority only.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn plan_index_mutation_for_slot_reader_structural(
    entity_tag: EntityTag,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    schema_info: &SchemaInfo,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, IndexPlanError> {
    plan_index_mutation_for_slot_reader_structural_impl(
        entity_tag,
        accepted_schema_fingerprint,
        mutation,
        schema_info,
        read_view,
        row_contract,
        old_primary_key,
        old_slots,
        new_primary_key,
        new_slots,
    )
}

// Keep the structural planner loop nongeneric once store lookup has already
// been lowered onto one index-store callback.
#[expect(clippy::too_many_arguments)]
fn plan_index_mutation_for_slot_reader_structural_impl(
    entity_tag: EntityTag,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    schema_info: &SchemaInfo,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    old_primary_key: Option<&PrimaryKeyValue>,
    mut old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    mut new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, IndexPlanError> {
    let accepted_expression_indexes = schema_info.expression_indexes();
    let mut groups = Vec::with_capacity(
        schema_info.field_path_indexes().len() + accepted_expression_indexes.len(),
    );

    for accepted_index in schema_info.field_path_indexes() {
        let predicate_program =
            accepted_index_mutation_predicate_program(accepted_index.predicate_sql(), row_contract);
        plan_accepted_field_path_index_mutation_for_slot_reader_structural(
            &mut groups,
            entity_tag,
            accepted_schema_fingerprint,
            mutation,
            read_view,
            row_contract,
            accepted_index,
            predicate_program.as_ref(),
            old_primary_key,
            old_slots
                .as_mut()
                .map(|slots| &mut **slots as &mut dyn CanonicalSlotReader),
            new_primary_key,
            new_slots
                .as_mut()
                .map(|slots| &mut **slots as &mut dyn CanonicalSlotReader),
        )?;
    }

    for accepted_index in accepted_expression_indexes {
        let predicate_program =
            accepted_index_mutation_predicate_program(accepted_index.predicate_sql(), row_contract);
        plan_accepted_expression_index_mutation_for_slot_reader_structural(
            &mut groups,
            entity_tag,
            accepted_schema_fingerprint,
            mutation,
            read_view,
            row_contract,
            accepted_index,
            predicate_program.as_ref(),
            old_primary_key,
            old_slots
                .as_mut()
                .map(|slots| &mut **slots as &mut dyn CanonicalSlotReader),
            new_primary_key,
            new_slots
                .as_mut()
                .map(|slots| &mut **slots as &mut dyn CanonicalSlotReader),
        )?;
    }

    Ok(IndexMutationPlan::new(groups))
}

#[expect(clippy::too_many_arguments)]
fn plan_accepted_field_path_index_mutation_for_slot_reader_structural(
    groups: &mut Vec<IndexDeltaGroup>,
    entity_tag: EntityTag,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    accepted_index: &SchemaIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<(), IndexPlanError> {
    let mut referenced_slots = vec![false; row_contract.field_count()];
    for field in accepted_index.fields() {
        if let Some(referenced) = referenced_slots.get_mut(field.slot()) {
            *referenced = true;
        }
    }
    if let Some(predicate_program) = predicate_program {
        predicate_program.mark_referenced_slots(&mut referenced_slots);
    }
    if unchanged_index_inputs(
        old_primary_key,
        old_slots
            .as_ref()
            .map(|slots| &**slots as &dyn CanonicalSlotReader),
        new_primary_key,
        new_slots
            .as_ref()
            .map(|slots| &**slots as &dyn CanonicalSlotReader),
        &referenced_slots,
    )? {
        return Ok(());
    }

    let index_store = accepted_index.store();
    let index_is_unique = accepted_index.unique();
    let read_contract = IndexReadContract::new(index_store, index_is_unique);
    let old_key = match old_slots {
        Some(slots) => load_structural_accepted_field_path_index_key(
            IndexKeyLane::Old,
            entity_tag,
            accepted_index,
            predicate_program,
            old_primary_key,
            slots,
        )?,
        None => None,
    };
    let new_key = match new_slots {
        Some(slots) => load_structural_accepted_field_path_index_key(
            IndexKeyLane::New,
            entity_tag,
            accepted_index,
            predicate_program,
            new_primary_key,
            slots,
        )?,
        None => None,
    };

    // Unchanged membership cannot conflict or alter the accepted index. Avoid
    // a stable index read and commit delta for unrelated-field updates; full
    // index integrity remains owned by the maintained integrity surfaces.
    if old_key.as_ref() == new_key.as_ref() && old_primary_key == new_primary_key {
        return Ok(());
    }

    let old_entry = load_existing_entry_structural(read_view, read_contract, old_key.as_ref())?;

    validate_existing_old_index_membership(old_primary_key, old_key.as_ref(), old_entry.as_ref())?;

    unique::validate_unique_constraint_accepted_field_path_structural(
        accepted_schema_fingerprint,
        mutation,
        entity_tag,
        read_view,
        row_contract,
        accepted_index,
        read_contract,
        new_key.as_ref().and(new_primary_key),
        new_key.as_ref(),
    )?;

    push_index_delta_group(
        groups,
        index_store,
        old_key,
        new_key,
        old_primary_key,
        new_primary_key,
    )?;

    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn plan_accepted_expression_index_mutation_for_slot_reader_structural(
    groups: &mut Vec<IndexDeltaGroup>,
    entity_tag: EntityTag,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    accepted_index: &SchemaExpressionIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<(), IndexPlanError> {
    let mut referenced_slots = vec![false; row_contract.field_count()];
    for item in accepted_index.key_items() {
        let field = match item {
            SchemaExpressionIndexKeyItemInfo::FieldPath(field) => field,
            SchemaExpressionIndexKeyItemInfo::Expression(expression) => expression.source(),
        };
        if let Some(referenced) = referenced_slots.get_mut(field.slot()) {
            *referenced = true;
        }
    }
    if let Some(predicate_program) = predicate_program {
        predicate_program.mark_referenced_slots(&mut referenced_slots);
    }
    if unchanged_index_inputs(
        old_primary_key,
        old_slots
            .as_ref()
            .map(|slots| &**slots as &dyn CanonicalSlotReader),
        new_primary_key,
        new_slots
            .as_ref()
            .map(|slots| &**slots as &dyn CanonicalSlotReader),
        &referenced_slots,
    )? {
        return Ok(());
    }

    let index_store = accepted_index.store();
    let index_is_unique = accepted_index.unique();
    let read_contract = IndexReadContract::new(index_store, index_is_unique);

    let old_key = match old_slots {
        Some(slots) => load_structural_accepted_expression_index_key(
            IndexKeyLane::Old,
            entity_tag,
            accepted_index,
            predicate_program,
            old_primary_key,
            slots,
        )?,
        None => None,
    };
    let new_key = match new_slots {
        Some(slots) => load_structural_accepted_expression_index_key(
            IndexKeyLane::New,
            entity_tag,
            accepted_index,
            predicate_program,
            new_primary_key,
            slots,
        )?,
        None => None,
    };

    if old_key.as_ref() == new_key.as_ref() && old_primary_key == new_primary_key {
        return Ok(());
    }

    let old_entry = load_existing_entry_structural(read_view, read_contract, old_key.as_ref())?;

    validate_existing_old_index_membership(old_primary_key, old_key.as_ref(), old_entry.as_ref())?;

    unique::validate_unique_constraint_accepted_expression_structural(
        accepted_schema_fingerprint,
        mutation,
        entity_tag,
        read_view,
        row_contract,
        accepted_index,
        read_contract,
        new_key.as_ref().and(new_primary_key),
        new_key.as_ref(),
    )?;

    push_index_delta_group(
        groups,
        index_store,
        old_key,
        new_key,
        old_primary_key,
        new_primary_key,
    )?;

    Ok(())
}

fn unchanged_index_inputs(
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&dyn CanonicalSlotReader>,
    referenced_slots: &[bool],
) -> Result<bool, InternalError> {
    let (Some(old_slots), Some(new_slots)) = (old_slots, new_slots) else {
        return Ok(false);
    };
    if old_primary_key != new_primary_key {
        return Ok(false);
    }
    for (slot, referenced) in referenced_slots.iter().copied().enumerate() {
        if referenced && old_slots.required_bytes(slot)? != new_slots.required_bytes(slot)? {
            return Ok(false);
        }
    }
    Ok(true)
}

// Convert one validated old/new key transition into index-domain membership
// deltas. Commit preparation later materializes these deltas against its active
// reader view, so this helper deliberately does not encode `IndexEntryValue`.
fn push_index_delta_group(
    groups: &mut Vec<IndexDeltaGroup>,
    index_store: &str,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_primary_key: Option<&PrimaryKeyValue>,
    new_primary_key: Option<&PrimaryKeyValue>,
) -> Result<(), InternalError> {
    // A row update that preserves both index key and row identity has already
    // validated the committed membership above. It needs no marker payload,
    // overlay entry, stable-index write, or index-generation movement.
    if old_key.as_ref() == new_key.as_ref() && old_primary_key == new_primary_key {
        return Ok(());
    }

    let mut deltas = Vec::with_capacity(2);

    if let Some(old_key) = old_key {
        let Some(old_primary_key) = old_primary_key else {
            return Err(InternalError::index_commit_op_old_entity_key_required());
        };
        deltas.push(IndexDelta::remove(old_key, old_primary_key));
    }

    if let Some(new_key) = new_key {
        let Some(new_primary_key) = new_primary_key else {
            return Err(InternalError::index_commit_op_new_entity_key_required());
        };
        deltas.push(IndexDelta::insert(new_key, new_primary_key));
    }

    if !deltas.is_empty() {
        groups.push(IndexDeltaGroup::new(index_store, deltas));
    }

    Ok(())
}

pub(super) fn load_existing_entry_structural(
    read_view: &dyn IndexPlanReadView,
    index: IndexReadContract<'_>,
    key: Option<&IndexKey>,
) -> Result<Option<IndexRowIdentity>, InternalError> {
    // No indexed key means no index entry to load.
    let Some(key) = key else {
        return Ok(None);
    };

    let raw_key = key.to_raw()?;

    read_view
        .read_index_entry(index, &raw_key)?
        .map(|raw_entry| {
            raw_entry
                .decode_row_identity(&raw_key)
                .map_err(|_| InternalError::structural_index_entry_corruption())
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        index::{IndexId, IndexKeyKind},
        key_taxonomy::PrimaryKeyComponent,
    };

    fn test_index_key(component: u8, primary_key: &PrimaryKeyValue) -> IndexKey {
        IndexKey::new_from_components_with_primary_key_value(
            &IndexId::new(EntityTag::new(73), 1),
            IndexKeyKind::User,
            &[vec![component]],
            primary_key,
        )
        .expect("test index key should encode")
    }

    #[test]
    fn unchanged_index_membership_emits_no_commit_delta_group() {
        let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Int64(41));
        let key = test_index_key(7, &primary_key);
        let mut groups = Vec::new();

        push_index_delta_group(
            &mut groups,
            "store",
            Some(key.clone()),
            Some(key),
            Some(&primary_key),
            Some(&primary_key),
        )
        .expect("unchanged membership should remain valid");

        assert!(groups.is_empty());
    }

    #[test]
    fn changed_index_membership_retains_remove_and_insert_deltas() {
        let primary_key = PrimaryKeyValue::from(PrimaryKeyComponent::Int64(41));
        let mut groups = Vec::new();

        push_index_delta_group(
            &mut groups,
            "store",
            Some(test_index_key(7, &primary_key)),
            Some(test_index_key(8, &primary_key)),
            Some(&primary_key),
            Some(&primary_key),
        )
        .expect("changed membership should remain valid");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].deltas.len(), 2);
    }
}
