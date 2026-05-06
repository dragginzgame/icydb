//! Module: relation::validate
//! Responsibility: relation integrity validation for strong relation delete/mutation safety.
//! Does not own: relation metadata derivation or reverse-index mutation construction.
//! Boundary: enforces relation target/source consistency before destructive operations.

use crate::{
    db::{
        Db,
        data::{DataKey, RawDataKey, StructuralRowContract},
        registry::StoreHandle,
        relation::{
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            metadata::{StrongRelationInfo, strong_relations_for_model_iter},
            model_has_strong_relations_to_target,
            reverse_index::{
                ReverseRelationSourceInfo, decode_relation_target_data_key, decode_reverse_entry,
                relation_target_store, reverse_index_key_for_target_storage_key,
                source_row_references_relation_target,
            },
        },
        schema::{AcceptedRowLayoutRuntimeDescriptor, ensure_accepted_schema_snapshot},
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::{StorageKey, storage_key_as_runtime_value},
};
use std::collections::BTreeSet;

///
/// BlockedDeleteProof
///
/// Structural proof payload returned by strong-relation delete validation.
/// This keeps the heavy blocked-delete scan nongeneric and leaves typed key
/// reconstruction at the final operator-facing diagnostic boundary only.
///

struct BlockedDeleteProof {
    relation: StrongRelationInfo,
    source_data_key: DataKey,
    target_key: StorageKey,
}

impl BlockedDeleteProof {
    // Rebuild the blocked-delete proof into the operator-facing unsupported
    // delete diagnostic at the final typed boundary.
    fn into_internal_error<S>(self) -> Result<InternalError, InternalError>
    where
        S: EntityKind + EntityValue,
    {
        Ok(InternalError::executor_unsupported(
            blocked_delete_diagnostic::<S>(
                self.relation,
                self.source_data_key.try_key::<S>()?,
                self.target_key,
            ),
        ))
    }
}

/// Validate that source rows do not strongly reference target keys selected for delete.
pub(in crate::db) fn validate_delete_strong_relations_for_source<S>(
    db: &Db<S::Canister>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    let source_info = ReverseRelationSourceInfo::for_type::<S>();

    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    // Phase 1: most source models do not own strong relations to the target
    // being deleted, so skip store resolution and proof setup entirely when
    // this source type cannot block the delete.
    if !model_has_strong_relations_to_target(S::MODEL, target_path) {
        return Ok(());
    }

    // Phase 2: resolve the source store once before the structural proof loop.
    let source_store = db.with_store_registry(|reg| reg.try_get_store(S::Store::PATH))?;
    let source_row_contract = accepted_source_row_contract::<S>(source_store)?;

    // Phase 3: run the heavy blocked-delete proof loop without `S`.
    let Some(blocked) = validate_delete_strong_relations_structural(
        db,
        source_info,
        S::PATH,
        S::MODEL,
        source_row_contract,
        target_path,
        source_store,
        deleted_target_keys,
    )?
    else {
        return Ok(());
    };

    // Phase 4: keep typed key reconstruction at the final diagnostic edge only.
    Err(blocked.into_internal_error::<S>()?)
}

/// Prove whether one delete would violate a strong source relation without `S`.
#[expect(clippy::too_many_arguments)]
fn validate_delete_strong_relations_structural<C>(
    db: &Db<C>,
    source_info: ReverseRelationSourceInfo,
    source_path: &'static str,
    source_model: &'static EntityModel,
    source_row_contract: StructuralRowContract,
    target_path: &str,
    source_store: StoreHandle,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<Option<BlockedDeleteProof>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve reverse-index candidates for each relevant relation field.
    for relation in strong_relations_for_model_iter(source_model, Some(target_path)) {
        let relation = relation.map_err(|err| {
            InternalError::strong_relation_target_name_invalid(
                source_path,
                err.field_name(),
                err.target_path(),
                err.target_entity_name(),
                err.source(),
            )
        })?;
        let target_index_store = relation_target_store(db, source_info, relation)?;

        for target_raw_key in deleted_target_keys {
            let Some(target_data_key) = decode_relation_target_data_key(
                source_info,
                relation,
                target_raw_key,
                RelationTargetDecodeContext::DeleteValidation,
                RelationTargetMismatchPolicy::Skip,
            )?
            else {
                continue;
            };
            let target_storage_key = target_data_key.storage_key();

            let Some(reverse_key) = reverse_index_key_for_target_storage_key(
                source_info,
                relation,
                target_storage_key,
            )?
            else {
                continue;
            };

            // Relation metrics are emitted as operation deltas so sink aggregation
            // always reflects the exact lookup/block operations performed.
            record(MetricsEvent::RelationValidation {
                entity_path: source_path,
                reverse_lookups: 1,
                blocked_deletes: 0,
            });

            let Some(raw_entry) = target_index_store.with_borrow(|store| store.get(&reverse_key))
            else {
                continue;
            };

            let entry = decode_reverse_entry(source_info, relation, &reverse_key, &raw_entry)?;

            // Phase 2: verify each candidate source row before rejecting delete.
            for source_key in entry.iter_ids() {
                let source_data_key = DataKey::new(source_info.entity_tag(), source_key);
                let source_raw_key = source_data_key.to_raw()?;
                let source_raw_row = source_store.with_data(|store| store.get(&source_raw_key));

                let Some(source_raw_row) = source_raw_row else {
                    let target = relation.target();
                    return Err(InternalError::reverse_index_entry_corrupted(
                        source_path,
                        relation.field_name,
                        target.path(),
                        &reverse_key,
                        format!(
                            "reverse index points at missing source row: source_id={source_key:?} key={:?}",
                            target_data_key.storage_key(),
                        ),
                    ));
                };

                let still_references_target = source_row_references_relation_target(
                    &source_raw_row,
                    source_row_contract.clone(),
                    source_info,
                    relation,
                    target_storage_key,
                )?;
                if still_references_target {
                    record(MetricsEvent::RelationValidation {
                        entity_path: source_path,
                        reverse_lookups: 0,
                        blocked_deletes: 1,
                    });

                    return Ok(Some(BlockedDeleteProof {
                        relation,
                        source_data_key,
                        target_key: target_storage_key,
                    }));
                }
            }
        }
    }

    Ok(None)
}

// Build the accepted-schema row contract used by delete relation validation.
//
// Relation validation reads source rows directly from storage, not from commit
// marker before-images. It must therefore decode old source rows through the
// accepted layout so appended nullable fields do not make unrelated relation
// checks fail on generated-only slot-count validation.
fn accepted_source_row_contract<S>(
    source_store: StoreHandle,
) -> Result<StructuralRowContract, InternalError>
where
    S: EntityKind,
{
    let accepted = source_store.with_schema_mut(|schema_store| {
        ensure_accepted_schema_snapshot(schema_store, S::ENTITY_TAG, S::PATH, S::MODEL)
    })?;
    let descriptor = AcceptedRowLayoutRuntimeDescriptor::from_accepted_schema(&accepted)?;
    descriptor.generated_compatible_row_shape_for_model(S::MODEL)?;

    Ok(
        StructuralRowContract::from_model_with_accepted_decode_contract(
            S::MODEL,
            descriptor.row_decode_contract(),
        ),
    )
}

/// Format operator-facing blocked-delete diagnostics with actionable context.
fn blocked_delete_diagnostic<S>(
    relation: StrongRelationInfo,
    source_key: S::Key,
    target_key: StorageKey,
) -> String
where
    S: EntityKind + EntityValue,
{
    format!(
        "delete blocked by strong relation: source_entity={} source_field={} source_id={source_key:?} target_entity={} target_key={:?}; action=delete source rows or retarget relation before deleting target",
        S::PATH,
        relation.field_name,
        relation.target().path(),
        storage_key_as_runtime_value(&target_key),
    )
}
