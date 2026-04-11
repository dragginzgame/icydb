//! Module: relation::save_validate
//! Responsibility: validate save-time strong relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates strong-relation target validation to this module.

use crate::{
    db::{
        Db,
        relation::{
            for_each_relation_target_value,
            metadata::{StrongRelationInfo, strong_relations_for_model_iter},
            raw_relation_target_key_from_value,
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
    for relation in strong_relations_for_model_iter(E::MODEL, None) {
        let value = entity
            .get_value_by_index(relation.field_index)
            .ok_or_else(|| {
                InternalError::executor_invariant(format!(
                    "entity field missing: {} field={}",
                    E::PATH,
                    relation.field_name
                ))
            })?;

        // Phase 2: validate each referenced relation key against target storage.
        for_each_relation_target_value(&value, |item| {
            validate_save_relation_value::<E>(db, relation, item)
        })?;
    }

    Ok(())
}

/// Validate one strong relation key against target-store membership.
fn validate_save_relation_value<E>(
    db: &Db<E::Canister>,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    // Phase 1: normalize relation key into canonical target raw-key form.
    let raw_key = raw_relation_target_key_from_value(
        relation.target_entity_tag,
        relation.target_entity_name,
        value,
    )
    .map_err(|err| {
        InternalError::relation_target_raw_key_error(
            err,
            E::PATH,
            relation.field_name,
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
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.field_name,
                relation.target_path,
                relation.target_store_path,
                value,
                err,
            )
        })?;
    let exists = store.with_data(|s| s.contains_key(&raw_key));
    if !exists {
        return Err(InternalError::strong_relation_target_missing(
            E::PATH,
            relation.field_name,
            relation.target_path,
            value,
        ));
    }

    Ok(())
}
