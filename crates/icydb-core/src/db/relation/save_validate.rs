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
            metadata::{
                StrongRelationInfo, StrongRelationMetadataError, strong_relations_for_model_iter,
            },
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
        let relation = relation.map_err(|err| strong_relation_metadata_error::<E>(&err))?;
        relation.validate_target_identity(db, E::PATH)?;
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
    let target = relation.target();
    let raw_key = raw_relation_target_key_from_value(target, value).map_err(|err| {
        InternalError::relation_target_raw_key_error(
            err,
            E::PATH,
            relation.field_name,
            target.path(),
            value,
            "strong relation key not storage-compatible",
        )
    })?;

    // Phase 2: resolve the target store and enforce key existence.
    let store = db
        .with_store_registry(|reg| reg.try_get_store(target.store_path()))
        .map_err(|err| {
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.field_name,
                target.path(),
                target.store_path(),
                value,
                err,
            )
        })?;
    let exists = store.with_data(|s| s.contains(&raw_key));
    if !exists {
        return Err(InternalError::strong_relation_target_missing(
            E::PATH,
            relation.field_name,
            target.path(),
            value,
        ));
    }

    Ok(())
}

// Map static metadata lowering failures at the save validation boundary where
// the typed source entity path is still available for diagnostics.
fn strong_relation_metadata_error<E>(err: &StrongRelationMetadataError) -> InternalError
where
    E: EntityKind + EntityValue,
{
    InternalError::strong_relation_target_name_invalid(
        E::PATH,
        err.field_name(),
        err.target_path(),
        err.target_entity_name(),
        err.source(),
    )
}
