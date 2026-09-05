//! Module: relation::validate
//! Responsibility: relation integrity validation for relation delete/mutation safety.
//! Does not own: relation metadata derivation or reverse-index mutation construction.
//! Boundary: enforces relation target/source consistency before destructive operations.

use crate::{
    db::{
        Db,
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, RawDataStoreKey,
            StructuralRowContract,
        },
        direction::Direction,
        index::StructuralPrimaryRowReader,
        registry::StoreHandle,
        relation::{
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            reverse_index::{
                AcceptedRelationInfo, RelationCommitBudget, ReverseRelationSourceInfo,
                accepted_relations_for_row_contract, decode_relation_target_data_key,
                decode_reverse_entry, relation_target_store,
                reverse_index_key_bounds_for_target_primary_key_value,
                source_row_references_relation_target_primary_key_value,
            },
        },
        runtime_entity_catalog::AcceptedRuntimeEntity,
    },
    error::{AcceptedConstraintFactContext, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{collections::BTreeSet, ops::Bound};

/// Validate that one accepted source has no accepted relation reference to
/// target keys selected for deletion.
pub(in crate::db) fn validate_delete_relations_for_accepted_source<C>(
    db: &Db<C>,
    source: &AcceptedRuntimeEntity,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
    source_reader: &dyn StructuralPrimaryRowReader,
    relation_budget: &mut RelationCommitBudget,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    let source_info =
        ReverseRelationSourceInfo::new(source.entity_path_handle(), source.entity_tag());

    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    let source_store = source.store(db)?;
    let (source_row_contract, accepted_schema_fingerprint) = accepted_source_row_contract(
        source_store,
        source.entity_tag(),
        source.entity_path(),
        source.store_path(),
    )?;
    let relations = accepted_relations_for_row_contract(
        db,
        source.entity_path(),
        &source_row_contract,
        Some(target_path),
    )?;
    if relations.is_empty() {
        return Ok(());
    }

    validate_delete_relations_structural(
        db,
        source_info,
        source.entity_path(),
        source_row_contract,
        accepted_schema_fingerprint,
        relations,
        deleted_target_keys,
        source_reader,
        relation_budget,
    )
}

/// Block target deletion while any matching candidate reverse generation is incomplete.
pub(in crate::db) fn validate_candidate_relation_target_delete_barrier<C: CanisterKind>(
    db: &Db<C>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
) -> Result<(), InternalError> {
    if deleted_target_keys.is_empty() {
        return Ok(());
    }
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    icydb_schema::compact_sort_unstable_by(&mut stores, |left, right| left.0.cmp(right.0));
    for (_, store) in stores {
        if let Some(barrier) = store.with_schema(|schema_store| {
            schema_store.pending_relation_activation_for_target(target_path)
        })? {
            return Err(InternalError::mutation_constraint_activation_write_blocked(
                AcceptedConstraintFactContext::write_admission(
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    barrier.accepted_schema_fingerprint(),
                    barrier.source_entity_tag().value(),
                    barrier.constraint_id().get(),
                    icydb_diagnostic_code::DiagnosticConstraintKind::Relation,
                    None,
                    None,
                ),
            ));
        }
    }
    Ok(())
}

/// Prove whether one delete would violate a source relation without `S`.
#[expect(
    clippy::too_many_arguments,
    reason = "delete validation combines the established relation read view with exact accepted diagnostic authority"
)]
fn validate_delete_relations_structural<C>(
    db: &Db<C>,
    source_info: ReverseRelationSourceInfo,
    source_path: &str,
    source_row_contract: StructuralRowContract,
    accepted_schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    relations: Vec<AcceptedRelationInfo>,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
    source_reader: &dyn StructuralPrimaryRowReader,
    relation_budget: &mut RelationCommitBudget,
) -> Result<(), InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve reverse-index candidates for each relevant relation field.
    for relation in relations {
        let target_index_store = relation_target_store(db, &source_info, &relation)?;

        for target_raw_key in deleted_target_keys {
            let Some(target_data_key) = decode_relation_target_data_key(
                &source_info,
                &relation,
                target_raw_key,
                RelationTargetDecodeContext::DeleteValidation,
                RelationTargetMismatchPolicy::Skip,
            )?
            else {
                continue;
            };
            let target_primary_key = target_data_key.primary_key_value();

            let Some((reverse_start, reverse_end)) =
                reverse_index_key_bounds_for_target_primary_key_value(
                    &source_info,
                    &relation,
                    &target_primary_key,
                )?
            else {
                continue;
            };

            let bounds = (Bound::Included(reverse_start), Bound::Excluded(reverse_end));
            target_index_store.with_borrow(|store| {
                store.visit_raw_entries_in_range(
                    (&bounds.0, &bounds.1),
                    Direction::Asc,
                    |reverse_key, raw_entry| {
                        let entry =
                            decode_reverse_entry(&source_info, &relation, reverse_key, raw_entry)?;

                        // Phase 2: verify the key-owned source row before rejecting delete.
                        let source_key = *entry.primary_key_value();
                        {
                            let source_data_key =
                                DecodedDataStoreKey::new(source_info.entity_tag(), &source_key);
                            let source_raw_row = source_reader.read_primary_row(&source_data_key)?;

                            let Some(source_raw_row) = source_raw_row else {
                                if source_reader
                                    .has_primary_row_override(&source_data_key)?
                                {
                                    // The canonical final overlay explicitly removed this
                                    // source, so its committed reverse-index witness cannot
                                    // block a target deleted by the same batch.
                                    return Ok(false);
                                }
                                let target = relation.target();
                                return Err(InternalError::reverse_index_entry_corrupted(
                                    source_path,
                                    relation.field_name(),
                                    target.path(),
                                    reverse_key,
                                    format!(
                                        "reverse index points at missing source row: source_id={source_key:?} key={target_primary_key:?}"
                                    ),
                                ));
                            };

                            let still_references_target =
                                source_row_references_relation_target_primary_key_value(
                                    &source_raw_row,
                                    source_row_contract.clone(),
                                    source_info.clone(),
                                    &relation,
                                    &target_primary_key,
                                    relation_budget,
                                )?;
                            if still_references_target {
                                return Err(relation.write_violation(
                                    accepted_schema_fingerprint,
                                    source_info.entity_tag(),
                                    None,
                                ));
                            }
                        }

                        Ok(false)
                    },
                )
            })?;
        }
    }

    Ok(())
}

// Build the accepted-schema row contract used by delete relation validation.
//
// Relation validation reads source rows directly from storage, not from commit
// marker before-images. It therefore decodes rows written at an earlier
// accepted append-only layout revision through the accepted layout, so newly
// appended nullable fields do not make unrelated relation checks fail.
fn accepted_source_row_contract(
    source_store: StoreHandle,
    source_tag: EntityTag,
    source_path: &str,
    source_store_path: &'static str,
) -> Result<
    (
        StructuralRowContract,
        crate::db::commit::CommitSchemaFingerprint,
    ),
    InternalError,
> {
    let selection = source_store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                source_tag,
                source_path,
                source_store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let fingerprint = selection.identity().accepted_schema_fingerprint();
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(source_path, &selection)
        .map(AcceptedStructuralRowAuthority::into_row_contract)?;
    Ok((contract, fingerprint))
}
