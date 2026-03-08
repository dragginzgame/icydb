//! Module: relation::save_validate
//! Responsibility: save-time strong relation existence/integrity validation.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates relation semantics to this module.

use crate::{
    db::{
        Db,
        relation::{
            StrongRelationTargetInfo, build_relation_target_raw_key,
            for_each_relation_target_value, incompatible_store_error,
            strong_relation_target_from_kind, target_key_mismatch_error,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

/// Validate strong relation references for one save candidate entity.
pub(in crate::db) fn validate_save_strong_relations<E>(
    db: &Db<E::Canister>,
    entity: &E,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: identify strong relation fields and read declared relation values.
    for (field_index, field) in E::MODEL.fields.iter().enumerate() {
        let Some(relation) = strong_relation_target_from_kind(&field.kind) else {
            continue;
        };

        let value = entity.get_value_by_index(field_index).ok_or_else(|| {
            InternalError::executor_invariant(format!(
                "entity field missing: {} field={}",
                E::PATH,
                field.name
            ))
        })?;

        // Phase 2: validate each referenced relation key against target storage.
        for_each_relation_target_value(&value, |item| {
            validate_save_relation_value::<E>(db, field.name, relation, item)
        })?;
    }

    Ok(())
}

/// Validate one strong relation key against target-store membership.
fn validate_save_relation_value<E>(
    db: &Db<E::Canister>,
    field_name: &str,
    relation: StrongRelationTargetInfo,
    value: &Value,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: normalize relation key into canonical target raw-key form.
    let raw_key =
        build_relation_target_raw_key(relation.target_entity_name, value).map_err(|err| {
            InternalError::relation_target_raw_key_error(
                err,
                E::PATH,
                field_name,
                relation.target_path,
                relation.target_entity_name,
                value,
                "strong relation key not storage-compatible",
                "strong relation target name invalid",
            )
        })?;

    // Phase 2: resolve the target store and enforce key existence.
    let store = db
        .with_store_registry(|reg| reg.try_get_store(relation.target_store_path))
        .map_err(|err| {
            incompatible_store_error(
                E::PATH,
                field_name,
                relation.target_path,
                relation.target_store_path,
                value,
                err,
            )
        })?;
    let exists = store.with_data(|s| s.contains_key(&raw_key));
    if !exists {
        return Err(target_key_mismatch_error(
            E::PATH,
            field_name,
            relation.target_path,
            value,
        ));
    }

    Ok(())
}
