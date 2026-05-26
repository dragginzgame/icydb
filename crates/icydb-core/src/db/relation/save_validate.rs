//! Module: relation::save_validate
//! Responsibility: validate save-time strong relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates strong-relation target validation to this module.

use crate::{
    db::{
        Db,
        relation::{
            AcceptedRelationTargetAuthority, accepted_relation_target_descriptor_from_kind,
            for_each_relation_target_value, validate_relation_primary_key_component_kind,
        },
        schema::{AcceptedRowDecodeContract, PersistedFieldKind, PersistedRelationStrength},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

// Save-time strong relation metadata projected from the accepted row contract.
// This is intentionally narrower than generated relation metadata: save
// validation only needs the source slot, source field name, and sealed target
// identity before it checks target-store membership.
struct AcceptedSaveStrongRelationInfo {
    field_index: usize,
    field_name: String,
    target: AcceptedRelationTargetAuthority,
}

impl AcceptedSaveStrongRelationInfo {
    fn validate_target_identity<C>(
        &self,
        db: &Db<C>,
        source_path: &str,
    ) -> Result<(), InternalError>
    where
        C: crate::traits::CanisterKind,
    {
        self.target
            .validate_against_db(db, source_path, self.field_name.as_str())
    }
}

/// Validate strong relation references through accepted schema metadata.
pub(in crate::db) fn validate_save_strong_relations_with_accepted_contract<E>(
    db: &Db<E::Canister>,
    entity: &E,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    for slot in 0..accepted_row_decode_contract.required_slot_count() {
        let Some(field) = accepted_row_decode_contract.field_for_slot(slot) else {
            continue;
        };
        let Some(relation) = accepted_save_strong_relation_from_field(
            E::PATH,
            slot,
            field.field_name(),
            field.kind(),
        )?
        else {
            continue;
        };

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

        for_each_relation_target_value(&value, |item| {
            validate_save_accepted_relation_value::<E>(db, &relation, item)
        })?;
    }

    Ok(())
}

fn accepted_save_strong_relation_from_field(
    source_path: &str,
    field_index: usize,
    field_name: &str,
    kind: &PersistedFieldKind,
) -> Result<Option<AcceptedSaveStrongRelationInfo>, InternalError> {
    let Some(target) = accepted_relation_target_descriptor_from_kind(kind) else {
        return Ok(None);
    };
    if target.strength != PersistedRelationStrength::Strong {
        return Ok(None);
    }
    validate_relation_primary_key_component_kind(target.target_key_kind)?;

    Ok(Some(AcceptedSaveStrongRelationInfo {
        field_index,
        field_name: field_name.to_string(),
        target: AcceptedRelationTargetAuthority::try_new(
            source_path,
            field_name,
            target.target_path,
            target.target_entity_name,
            target.target_entity_tag,
            target.target_store_path,
        )?,
    }))
}

fn validate_save_accepted_relation_value<E>(
    db: &Db<E::Canister>,
    relation: &AcceptedSaveStrongRelationInfo,
    value: &Value,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    let Some(component) = crate::db::key_taxonomy::PrimaryKeyComponent::from_runtime_value(value)
    else {
        return Err(InternalError::relation_target_raw_key_error(
            E::PATH,
            relation.field_name.as_str(),
            relation.target.path(),
            value,
            "strong relation target key unsupported",
        ));
    };
    let raw_key = crate::db::data::DecodedDataStoreKey::new(
        relation.target.entity_tag(),
        &crate::db::key_taxonomy::PrimaryKeyValue::Scalar(component),
    )
    .to_raw()
    .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;
    let target_store = db
        .with_store_registry(|registry| registry.try_get_store(relation.target.store_path()))
        .map_err(|err| {
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.field_name.as_str(),
                relation.target.path(),
                relation.target.store_path(),
                value,
                err,
            )
        })?;
    let target_exists = target_store
        .data_store()
        .with_borrow(|store| store.get(&raw_key).is_some());

    if target_exists {
        Ok(())
    } else {
        Err(InternalError::strong_relation_target_missing(
            E::PATH,
            relation.field_name.as_str(),
            relation.target.path(),
            value,
        ))
    }
}
