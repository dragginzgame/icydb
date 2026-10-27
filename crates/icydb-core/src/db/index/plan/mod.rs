//! Module: index::plan
//! Responsibility: preflight planning for deterministic index mutations.
//! Does not own: commit marker protocol or runtime apply sequencing.
//! Boundary: executor/commit call this module before writing commit markers.

mod delta;
mod error;
mod integrity;
mod metadata;
mod read;
mod unique;

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::{CanonicalSlotReader, StructuralRowContract},
        index::{IndexId, IndexKey, IndexReadContract},
        key_taxonomy::PrimaryKeyValue,
        predicate::PredicateProgram,
        schema::{SchemaExpressionIndexInfo, SchemaIndexInfo},
    },
    error::{InternalError, MutationDiagnosticContext},
    types::EntityTag,
};

pub(in crate::db) use delta::{
    IndexDelta, IndexDeltaGroup, IndexMembershipDelta, IndexMutationPlan,
};
pub(in crate::db) use integrity::{AcceptedIndexInspectionDomain, AcceptedIndexInspectionPlan};
pub(in crate::db) use metadata::PreparedIndexMutationMetadata;
pub(in crate::db) use read::IndexPlanReadView;

use metadata::PreparedIndexInputs;

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

// The existing two-family key authority now serves mutation and uniqueness.
// Key construction stays specialized; transition ordering has one owner.
enum AcceptedIndexKeyAuthority<'a> {
    AcceptedFieldPath(&'a SchemaIndexInfo, Option<&'a PredicateProgram>),
    AcceptedExpression(&'a SchemaExpressionIndexInfo, Option<&'a PredicateProgram>),
}

impl<'a> AcceptedIndexKeyAuthority<'a> {
    const fn index_id(&self, entity_tag: EntityTag) -> IndexId {
        match self {
            Self::AcceptedFieldPath(index, _) => IndexId::new_with_generation(
                entity_tag,
                index.ordinal(),
                index.physical_generation(),
            ),
            Self::AcceptedExpression(index, _) => IndexId::new_with_generation(
                entity_tag,
                index.ordinal(),
                index.physical_generation(),
            ),
        }
    }

    const fn read_contract(&self) -> IndexReadContract<'a> {
        match self {
            Self::AcceptedFieldPath(index, _) => {
                IndexReadContract::new(index.store(), index.unique())
            }
            Self::AcceptedExpression(index, _) => {
                IndexReadContract::new(index.store(), index.unique())
            }
        }
    }

    fn load_key(
        &self,
        lane: IndexKeyLane,
        entity_tag: EntityTag,
        primary_key: Option<&PrimaryKeyValue>,
        slots: &dyn CanonicalSlotReader,
    ) -> Result<Option<IndexKey>, InternalError> {
        let primary_key = primary_key.ok_or_else(|| lane.missing_entity_key_error())?;

        self.build_index_key_from_row_slots(entity_tag, primary_key, slots)
    }

    fn build_index_key_from_row_slots(
        &self,
        entity_tag: EntityTag,
        primary_key: &PrimaryKeyValue,
        row_fields: &dyn CanonicalSlotReader,
    ) -> Result<Option<IndexKey>, InternalError> {
        let predicate_program = match self {
            Self::AcceptedFieldPath(_, program) | Self::AcceptedExpression(_, program) => *program,
        };
        // Membership admission precedes either specialized constructor. A
        // rejected row must not read/encode index components on either route.
        if let Some(program) = predicate_program
            && !program.eval_with_structural_slot_reader(row_fields)?
        {
            return Ok(None);
        }
        match self {
            Self::AcceptedFieldPath(index, _) => {
                IndexKey::new_from_slots_with_accepted_field_path_index_primary_key_value(
                    entity_tag,
                    primary_key,
                    index,
                    row_fields,
                )
            }
            Self::AcceptedExpression(index, _) => {
                IndexKey::new_from_slots_with_accepted_expression_index_primary_key_value(
                    entity_tag,
                    primary_key,
                    index,
                    row_fields,
                )
            }
        }
    }

    fn unique_violation(
        &self,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
        mutation: Option<MutationDiagnosticContext>,
        entity_tag: EntityTag,
    ) -> InternalError {
        let identity = match self {
            Self::AcceptedFieldPath(index, _) => index.unique_constraint(),
            Self::AcceptedExpression(index, _) => index.unique_constraint(),
        };
        let Some(identity) = identity else {
            return InternalError::index_unique_validation_corruption();
        };
        error::unique_violation(
            accepted_schema_fingerprint,
            mutation,
            identity.id().get(),
            entity_tag.value(),
        )
    }
}

pub(in crate::db::index::plan) fn accepted_field_path_index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    accepted_index: &SchemaIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: &PrimaryKeyValue,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    AcceptedIndexKeyAuthority::AcceptedFieldPath(accepted_index, predicate_program)
        .build_index_key_from_row_slots(entity_tag, primary_key, slots)
}

pub(in crate::db::index::plan) fn accepted_expression_index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    accepted_index: &SchemaExpressionIndexInfo,
    predicate_program: Option<&PredicateProgram>,
    primary_key: &PrimaryKeyValue,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    AcceptedIndexKeyAuthority::AcceptedExpression(accepted_index, predicate_program)
        .build_index_key_from_row_slots(entity_tag, primary_key, slots)
}

// Prove that the pre-existing old index entry still contains the expected row
// membership before commit planning becomes purely mechanical.
fn validate_existing_old_index_membership(
    old_primary_key: Option<&PrimaryKeyValue>,
    old_key: Option<&IndexKey>,
    old_entry: Option<&PrimaryKeyValue>,
) -> Result<(), InternalError> {
    if old_key.is_none() {
        return Ok(());
    }

    let Some(old_primary_key) = old_primary_key else {
        return Err(InternalError::structural_index_removal_entity_key_required());
    };

    let entry = old_entry.ok_or_else(InternalError::structural_index_entry_corruption)?;

    if entry != old_primary_key {
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
    metadata: &PreparedIndexMutationMetadata,
    read_view: &dyn IndexPlanReadView,
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, InternalError> {
    let schema_info = &metadata.schema_info;
    let row_contract = &metadata.row_contract;
    let accepted_expression_indexes = schema_info.expression_indexes();
    let mut groups = Vec::with_capacity(
        schema_info.field_path_indexes().len() + accepted_expression_indexes.len(),
    );

    for (accepted_index, inputs) in schema_info
        .field_path_indexes()
        .iter()
        .zip(&metadata.field_path_inputs)
    {
        plan_accepted_index_mutation_for_slot_reader_structural(
            &mut groups,
            entity_tag,
            accepted_schema_fingerprint,
            mutation,
            read_view,
            row_contract,
            AcceptedIndexKeyAuthority::AcceptedFieldPath(
                accepted_index,
                inputs.predicate_program.as_ref(),
            ),
            inputs,
            old_primary_key,
            old_slots,
            new_primary_key,
            new_slots,
        )?;
    }

    for (accepted_index, inputs) in accepted_expression_indexes
        .iter()
        .zip(&metadata.expression_inputs)
    {
        plan_accepted_index_mutation_for_slot_reader_structural(
            &mut groups,
            entity_tag,
            accepted_schema_fingerprint,
            mutation,
            read_view,
            row_contract,
            AcceptedIndexKeyAuthority::AcceptedExpression(
                accepted_index,
                inputs.predicate_program.as_ref(),
            ),
            inputs,
            old_primary_key,
            old_slots,
            new_primary_key,
            new_slots,
        )?;
    }

    Ok(IndexMutationPlan::new(groups))
}

#[expect(clippy::too_many_arguments)]
fn plan_accepted_index_mutation_for_slot_reader_structural(
    groups: &mut Vec<IndexDeltaGroup>,
    entity_tag: EntityTag,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    mutation: Option<MutationDiagnosticContext>,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    accepted_index: AcceptedIndexKeyAuthority<'_>,
    inputs: &PreparedIndexInputs,
    old_primary_key: Option<&PrimaryKeyValue>,
    old_slots: Option<&dyn CanonicalSlotReader>,
    new_primary_key: Option<&PrimaryKeyValue>,
    new_slots: Option<&dyn CanonicalSlotReader>,
) -> Result<(), InternalError> {
    // Verification rebuilds every after-image key. Admit it even when the
    // indexed inputs are unchanged, then reuse it without reading index state.
    let new_key = match new_slots {
        Some(slots) => {
            accepted_index.load_key(IndexKeyLane::New, entity_tag, new_primary_key, slots)?
        }
        None => None,
    };
    if unchanged_index_inputs(
        old_primary_key,
        old_slots,
        new_primary_key,
        new_slots,
        &inputs.referenced_slots,
    )? {
        return Ok(());
    }

    let read_contract = accepted_index.read_contract();
    let index_store = read_contract.store_path();
    let old_key = match old_slots {
        Some(slots) => {
            accepted_index.load_key(IndexKeyLane::Old, entity_tag, old_primary_key, slots)?
        }
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

    unique::validate_unique_constraint_structural_impl(
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
        let Some(_) = old_primary_key else {
            return Err(InternalError::index_commit_op_old_entity_key_required());
        };
        deltas.push(IndexDelta::remove(old_key));
    }

    if let Some(new_key) = new_key {
        let Some(_) = new_primary_key else {
            return Err(InternalError::index_commit_op_new_entity_key_required());
        };
        deltas.push(IndexDelta::insert(new_key));
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
) -> Result<Option<PrimaryKeyValue>, InternalError> {
    // No indexed key means no index entry to load.
    let Some(key) = key else {
        return Ok(None);
    };

    let raw_key = key.to_raw()?;

    read_view
        .read_index_entry(index, &raw_key)?
        .map(|raw_entry| {
            raw_entry
                .decode_row_witness_from_index_key(key)
                .map(|witness| *witness.primary_key_value())
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
