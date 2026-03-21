//! Module: relation::validate
//! Responsibility: relation integrity validation for strong relation delete/mutation safety.
//! Does not own: relation metadata derivation or reverse-index mutation construction.
//! Boundary: enforces relation target/source consistency before destructive operations.

use crate::{
    db::{
        Db,
        data::{DataKey, RawDataKey},
        registry::StoreHandle,
        relation::{
            RelationTargetDecodeContext, RelationTargetMismatchPolicy,
            metadata::{StrongRelationInfo, strong_relations_for_source},
            reverse_index::{
                ReverseRelationSourceInfo, decode_relation_target_data_key_for_relation,
                decode_reverse_entry, relation_target_keys_for_source_row, relation_target_store,
                reverse_index_key_for_target_value,
            },
        },
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    model::entity::EntityModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
    value::Value,
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
    target_value: Value,
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

    // Phase 1: resolve the structural relation descriptors and source store once.
    let relations = strong_relations_for_source::<S>(Some(target_path));
    if relations.is_empty() {
        return Ok(());
    }
    let source_store = db.with_store_registry(|reg| reg.try_get_store(S::Store::PATH))?;

    // Phase 2: run the heavy blocked-delete proof loop without `S`.
    let Some(blocked) = validate_delete_strong_relations_structural(
        db,
        source_info,
        S::PATH,
        S::MODEL,
        source_store,
        &relations,
        deleted_target_keys,
    )?
    else {
        return Ok(());
    };

    // Phase 3: keep typed key reconstruction at the final diagnostic edge only.
    Err(crate::db::error::executor_unsupported(
        blocked_delete_diagnostic::<S>(
            blocked.relation,
            blocked.source_data_key.try_key::<S>()?,
            &blocked.target_value,
        ),
    ))
}

/// Prove whether one delete would violate a strong source relation without `S`.
fn validate_delete_strong_relations_structural<C>(
    db: &Db<C>,
    source_info: ReverseRelationSourceInfo,
    source_path: &'static str,
    source_model: &'static EntityModel,
    source_store: StoreHandle,
    relations: &[StrongRelationInfo],
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<Option<BlockedDeleteProof>, InternalError>
where
    C: CanisterKind,
{
    // Phase 1: resolve reverse-index candidates for each relevant relation field.
    for relation in relations {
        let target_index_store = relation_target_store(db, source_info, *relation)?;

        for target_raw_key in deleted_target_keys {
            let Some(target_data_key) = decode_relation_target_data_key_for_relation(
                source_info,
                *relation,
                target_raw_key,
                RelationTargetDecodeContext::DeleteValidation,
                RelationTargetMismatchPolicy::Skip,
            )?
            else {
                continue;
            };

            let target_value = target_data_key.storage_key().as_value();
            let Some(reverse_key) =
                reverse_index_key_for_target_value(source_info, *relation, &target_value)?
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

            let entry = decode_reverse_entry(source_info, *relation, &reverse_key, &raw_entry)?;

            // Phase 2: verify each candidate source row before rejecting delete.
            for source_key in entry.iter_ids() {
                let source_data_key = DataKey::new(source_info.entity_tag(), source_key);
                let source_raw_key = source_data_key.to_raw()?;
                let source_raw_row = source_store.with_data(|store| store.get(&source_raw_key));

                let Some(source_raw_row) = source_raw_row else {
                    return Err(InternalError::store_corruption(format!(
                        "reverse index points at missing source row: source={} field={} source_id={source_key:?} target={} key={target_value:?}",
                        source_path, relation.field_name, relation.target_path,
                    )));
                };

                let source_targets = relation_target_keys_for_source_row(
                    &source_raw_row,
                    source_model,
                    source_info,
                    *relation,
                )?;
                if source_targets.contains(target_raw_key) {
                    record(MetricsEvent::RelationValidation {
                        entity_path: source_path,
                        reverse_lookups: 0,
                        blocked_deletes: 1,
                    });

                    return Ok(Some(BlockedDeleteProof {
                        relation: *relation,
                        source_data_key,
                        target_value,
                    }));
                }
            }
        }
    }

    Ok(None)
}

/// Format operator-facing blocked-delete diagnostics with actionable context.
fn blocked_delete_diagnostic<S>(
    relation: StrongRelationInfo,
    source_key: S::Key,
    target_value: &Value,
) -> String
where
    S: EntityKind + EntityValue,
{
    format!(
        "delete blocked by strong relation: source_entity={} source_field={} source_id={source_key:?} target_entity={} target_key={target_value:?}; action=delete source rows or retarget relation before deleting target",
        S::PATH,
        relation.field_name,
        relation.target_path,
    )
}
