//! Module: index::plan
//! Responsibility: preflight planning for deterministic index mutations.
//! Does not own: commit marker protocol or runtime apply sequencing.
//! Boundary: executor/commit call this module before writing commit markers.

mod delta;
mod error;
mod read;
mod unique;

use crate::{
    db::{
        data::{CanonicalSlotReader, StorageKey, StructuralRowContract},
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexReadContract,
            canonical_index_predicate,
        },
        predicate::PredicateProgram,
        schema::{SchemaIndexInfo, SchemaInfo},
    },
    error::InternalError,
    model::{entity::EntityModel, index::IndexModel},
    types::EntityTag,
};
use error::IndexPlanError;

pub(in crate::db) use delta::{
    IndexDelta, IndexDeltaGroup, IndexMembershipDelta, IndexMutationPlan,
};
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

// Format the canonical human-readable index field list once at the plan boundary.
pub(super) fn index_fields_csv(index: &IndexModel) -> String {
    index.fields().join(", ")
}

// Format accepted field-path key components for corruption diagnostics without
// reopening generated index field declarations.
fn accepted_index_fields_csv(index: &SchemaIndexInfo) -> String {
    index
        .fields()
        .iter()
        .map(|field| field.path().join("."))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Compile the optional conditional-index predicate from structural entity
/// authority only.
pub(in crate::db) fn compile_index_membership_predicate_structural(
    _entity_path: &'static str,
    index: &IndexModel,
    row_contract: &StructuralRowContract,
) -> Option<PredicateProgram> {
    let predicate = canonical_index_predicate(index)?;

    Some(PredicateProgram::compile_with_row_contract(
        row_contract,
        predicate,
    ))
}

fn accepted_field_path_index_for_generated_index<'a>(
    schema_info: &'a SchemaInfo,
    index: &IndexModel,
    entity_path: &'static str,
) -> Result<Option<&'a SchemaIndexInfo>, IndexPlanError> {
    if index.has_expression_key_items() {
        return Ok(None);
    }

    schema_info
        .field_path_indexes()
        .iter()
        .find(|accepted| accepted.name() == index.name())
        .map(Some)
        .ok_or_else(|| {
            InternalError::index_plan_index_corruption(format!(
                "missing accepted index contract for '{entity_path}' index '{}'",
                index.name(),
            ))
            .into()
        })
}

/// Build one index key from one slot reader using accepted row-contract slot authority.
pub(in crate::db) fn index_key_for_slot_reader_with_membership_structural(
    entity_tag: EntityTag,
    index: &IndexModel,
    accepted_index: Option<&SchemaIndexInfo>,
    row_contract: &StructuralRowContract,
    predicate_program: Option<&PredicateProgram>,
    storage_key: StorageKey,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    if let Some(predicate_program) = predicate_program {
        let keep_row = predicate_program.eval_with_structural_slot_reader(slots)?;
        if !keep_row {
            return Ok(None);
        }
    }

    let index_key = if let Some(accepted_index) = accepted_index {
        IndexKey::new_from_slots_with_accepted_field_path_index(
            entity_tag,
            storage_key,
            accepted_index,
            slots,
        )?
    } else {
        IndexKey::new_from_slots_with_contract(entity_tag, storage_key, row_contract, slots, index)?
    };

    Ok(index_key)
}

// Build one optional structural index key for the requested planner lane.
#[expect(clippy::too_many_arguments)]
fn load_structural_index_key(
    lane: IndexKeyLane,
    entity_tag: EntityTag,
    index: &IndexModel,
    accepted_index: Option<&SchemaIndexInfo>,
    row_contract: &StructuralRowContract,
    predicate_program: Option<&PredicateProgram>,
    storage_key: Option<StorageKey>,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<IndexKey>, InternalError> {
    let Some(storage_key) = storage_key else {
        return Err(lane.missing_entity_key_error());
    };

    index_key_for_slot_reader_with_membership_structural(
        entity_tag,
        index,
        accepted_index,
        row_contract,
        predicate_program,
        storage_key,
        slots,
    )
}

// Prove that the pre-existing old index entry still contains the expected row
// membership before commit planning becomes purely mechanical.
fn validate_existing_old_index_membership(
    entity_path: &'static str,
    index_fields: &str,
    index_is_unique: bool,
    old_storage_key: Option<StorageKey>,
    old_key: Option<&IndexKey>,
    old_entry: Option<&IndexEntry>,
) -> Result<(), InternalError> {
    let Some(old_key) = old_key else {
        return Ok(());
    };

    let Some(old_storage_key) = old_storage_key else {
        return Err(InternalError::structural_index_removal_entity_key_required());
    };

    let entry = old_entry.as_ref().ok_or_else(|| {
        InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::missing_key(old_key.to_raw(), old_storage_key),
        )
    })?;

    if index_is_unique && entry.len() > 1 {
        return Err(InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::NonUniqueEntry { keys: entry.len() },
        ));
    }

    if !entry.contains(old_storage_key) {
        return Err(InternalError::structural_index_entry_corruption(
            entity_path,
            index_fields,
            IndexEntryCorruption::missing_key(old_key.to_raw(), old_storage_key),
        ));
    }

    Ok(())
}

/// Plan all index mutations for one persisted-row transition using structural
/// entity authority only.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn plan_index_mutation_for_slot_reader_structural(
    entity_path: &'static str,
    entity_tag: EntityTag,
    model: &'static EntityModel,
    schema_info: &SchemaInfo,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    old_storage_key: Option<StorageKey>,
    old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_storage_key: Option<StorageKey>,
    new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, IndexPlanError> {
    plan_index_mutation_for_slot_reader_structural_impl(
        entity_path,
        entity_tag,
        model,
        schema_info,
        read_view,
        row_contract,
        old_storage_key,
        old_slots,
        new_storage_key,
        new_slots,
    )
}

// Keep the structural planner loop nongeneric once store lookup has already
// been lowered onto one index-store callback.
#[expect(clippy::too_many_arguments)]
fn plan_index_mutation_for_slot_reader_structural_impl(
    entity_path: &'static str,
    entity_tag: EntityTag,
    model: &'static EntityModel,
    schema_info: &SchemaInfo,
    read_view: &dyn IndexPlanReadView,
    row_contract: &StructuralRowContract,
    old_storage_key: Option<StorageKey>,
    mut old_slots: Option<&mut dyn CanonicalSlotReader>,
    new_storage_key: Option<StorageKey>,
    mut new_slots: Option<&mut dyn CanonicalSlotReader>,
) -> Result<IndexMutationPlan, IndexPlanError> {
    let indexes = model.indexes();
    let mut groups = Vec::with_capacity(indexes.len());

    // Phase 1: per-index load, validate, and synthesize index-domain deltas
    // from slot-reader projections only.
    for index in indexes {
        let accepted_index =
            accepted_field_path_index_for_generated_index(schema_info, index, entity_path)?;
        let index_fields =
            accepted_index.map_or_else(|| index_fields_csv(index), accepted_index_fields_csv);
        let index_store = accepted_index.map_or_else(|| index.store(), SchemaIndexInfo::store);
        let index_is_unique =
            accepted_index.map_or_else(|| index.is_unique(), SchemaIndexInfo::unique);
        let read_contract = IndexReadContract::new(index_store, index_is_unique, &index_fields);
        let membership_program =
            compile_index_membership_predicate_structural(entity_path, index, row_contract);

        let old_key = match old_slots.as_deref_mut() {
            Some(slots) => load_structural_index_key(
                IndexKeyLane::Old,
                entity_tag,
                index,
                accepted_index,
                row_contract,
                membership_program.as_ref(),
                old_storage_key,
                slots,
            )?,
            None => None,
        };
        let new_key = match new_slots.as_deref_mut() {
            Some(slots) => load_structural_index_key(
                IndexKeyLane::New,
                entity_tag,
                index,
                accepted_index,
                row_contract,
                membership_program.as_ref(),
                new_storage_key,
                slots,
            )?,
            None => None,
        };

        let old_entry = load_existing_entry_structural(
            read_view,
            read_contract,
            &index_fields,
            old_key.as_ref(),
            entity_path,
        )?;

        // Phase 2: ensure any existing old membership is still present before
        // commit-phase mutations become mechanical.
        validate_existing_old_index_membership(
            entity_path,
            &index_fields,
            index_is_unique,
            old_storage_key,
            old_key.as_ref(),
            old_entry.as_ref(),
        )?;

        unique::validate_unique_constraint_structural(
            entity_path,
            entity_tag,
            read_view,
            row_contract,
            index,
            accepted_index,
            read_contract,
            &index_fields,
            if new_key.is_some() {
                new_storage_key
            } else {
                None
            },
            new_key.as_ref(),
        )?;

        push_index_delta_group(
            &mut groups,
            index_store,
            index_fields,
            old_key,
            new_key,
            old_storage_key,
            new_storage_key,
        )?;
    }

    Ok(IndexMutationPlan::new(groups))
}

// Convert one validated old/new key transition into index-domain membership
// deltas. Commit preparation later materializes these deltas against its active
// reader view, so this helper deliberately does not encode `RawIndexEntry`.
fn push_index_delta_group(
    groups: &mut Vec<IndexDeltaGroup>,
    index_store: &str,
    index_fields: String,
    old_key: Option<IndexKey>,
    new_key: Option<IndexKey>,
    old_storage_key: Option<StorageKey>,
    new_storage_key: Option<StorageKey>,
) -> Result<(), InternalError> {
    let mut deltas = Vec::with_capacity(2);

    if let Some(old_key) = old_key {
        let Some(old_storage_key) = old_storage_key else {
            return Err(InternalError::index_commit_op_old_entity_key_required());
        };
        deltas.push(IndexDelta::remove(old_key, old_storage_key));
    }

    if let Some(new_key) = new_key {
        let Some(new_storage_key) = new_storage_key else {
            return Err(InternalError::index_commit_op_new_entity_key_required());
        };
        deltas.push(IndexDelta::insert(new_key, new_storage_key));
    }

    if !deltas.is_empty() {
        groups.push(IndexDeltaGroup::new(index_store, index_fields, deltas));
    }

    Ok(())
}

pub(super) fn load_existing_entry_structural(
    read_view: &dyn IndexPlanReadView,
    index: IndexReadContract<'_>,
    index_fields: &str,
    key: Option<&IndexKey>,
    entity_path: &'static str,
) -> Result<Option<IndexEntry>, InternalError> {
    // No indexed key means no index entry to load.
    let Some(key) = key else {
        return Ok(None);
    };

    let raw_key = key.to_raw();

    read_view
        .read_index_entry(index, &raw_key)?
        .map(|raw_entry| {
            raw_entry.try_decode().map_err(|err| {
                InternalError::structural_index_entry_corruption(entity_path, index_fields, err)
            })
        })
        .transpose()
}
