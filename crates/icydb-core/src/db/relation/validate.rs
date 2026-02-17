use super::{
    RelationTargetDecodeContext, RelationTargetMismatchPolicy,
    decode_relation_target_data_key_for_relation,
    metadata::{StrongRelationInfo, strong_relations_for_source},
    reverse_index::{
        decode_reverse_entry, relation_target_keys_for_source, relation_target_store,
        reverse_index_key_for_target_value,
    },
};
use crate::{
    db::{
        Db,
        store::{DataKey, RawDataKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{MetricsEvent, record},
    traits::{EntityKind, EntityValue, Path},
    value::Value,
};
use std::collections::BTreeSet;

/// Validate that source rows do not strongly reference target keys selected for delete.
pub fn validate_delete_strong_relations_for_source<S>(
    db: &Db<S::Canister>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    let relations = strong_relations_for_source::<S>(Some(target_path));
    if relations.is_empty() {
        return Ok(());
    }
    let source_store = db.with_store_registry(|reg| reg.try_get_store(S::Store::PATH))?;

    // Phase 1: resolve reverse-index candidates for each relevant relation field.
    for relation in relations {
        let target_index_store = relation_target_store::<S>(db, relation)?;

        for target_raw_key in deleted_target_keys {
            let Some(target_data_key) = decode_relation_target_data_key_for_relation::<S>(
                relation,
                target_raw_key,
                RelationTargetDecodeContext::DeleteValidation,
                RelationTargetMismatchPolicy::Skip,
            )?
            else {
                continue;
            };

            let target_value = target_data_key.storage_key().as_value();
            let Some(reverse_key) =
                reverse_index_key_for_target_value::<S>(relation, &target_value)?
            else {
                continue;
            };

            // Relation metrics are emitted as operation deltas so sink aggregation
            // always reflects the exact lookup/block operations performed.
            record(MetricsEvent::RelationValidation {
                entity_path: S::PATH,
                reverse_lookups: 1,
                blocked_deletes: 0,
            });

            let Some(raw_entry) = target_index_store.with_borrow(|store| store.get(&reverse_key))
            else {
                continue;
            };

            let entry = decode_reverse_entry::<S>(relation, &reverse_key, &raw_entry)?;

            // Phase 2: verify each candidate source row before rejecting delete.
            for source_key in entry.iter_ids() {
                let source_data_key = DataKey::try_new::<S>(source_key)?;
                let source_raw_key = source_data_key.to_raw()?;
                let source_raw_row = source_store.with_data(|store| store.get(&source_raw_key));

                let Some(source_raw_row) = source_raw_row else {
                    return Err(InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Store,
                        format!(
                            "reverse index points at missing source row: source={} field={} source_id={source_key:?} target={} key={target_value:?}",
                            S::PATH,
                            relation.field_name,
                            relation.target_path,
                        ),
                    ));
                };

                let source = source_raw_row.try_decode::<S>().map_err(|err| {
                    InternalError::new(
                        ErrorClass::Corruption,
                        ErrorOrigin::Serialize,
                        format!(
                            "source row decode failed during delete relation validation: source={} ({err})",
                            S::PATH
                        ),
                    )
                })?;

                let source_targets = relation_target_keys_for_source(&source, relation)?;
                if source_targets.contains(target_raw_key) {
                    record(MetricsEvent::RelationValidation {
                        entity_path: S::PATH,
                        reverse_lookups: 0,
                        blocked_deletes: 1,
                    });
                    return Err(InternalError::new(
                        ErrorClass::Unsupported,
                        ErrorOrigin::Executor,
                        blocked_delete_diagnostic::<S>(relation, source_key, &target_value),
                    ));
                }
            }
        }
    }

    Ok(())
}

// Format operator-facing blocked-delete diagnostics with actionable context.
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
