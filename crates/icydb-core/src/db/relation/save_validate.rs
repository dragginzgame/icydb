//! Module: relation::save_validate
//! Responsibility: validate save-time strong relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates strong-relation target validation to this module.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        registry::StoreHandle,
        relation::{
            AcceptedRelationTargetAuthority, accepted_relation_target_descriptor_from_kind,
            for_each_relation_target_value, validate_relation_primary_key_component_kind,
        },
        schema::{
            AcceptedRowDecodeContract, PersistedFieldKind, PersistedRelationStrength,
            ensure_accepted_schema_snapshot,
        },
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
    scalar_target_key_kind: PersistedFieldKind,
}

impl AcceptedSaveStrongRelationInfo {
    fn validate_target_identity<'db, C>(
        &self,
        db: &'db Db<C>,
        source_path: &str,
    ) -> Result<Option<&'db EntityRuntimeHooks<C>>, InternalError>
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

        let value = entity
            .get_value_by_index(relation.field_index)
            .ok_or_else(|| {
                InternalError::executor_invariant(format!(
                    "entity field missing: {} field={}",
                    E::PATH,
                    relation.field_name
                ))
            })?;

        let target_hook = relation.validate_target_identity(db, E::PATH)?;
        let target_store = target_store_for_relation::<E>(db, &relation, &value)?;
        if let Some(target_hook) = target_hook {
            validate_target_accepted_scalar_primary_key::<E::Canister>(
                E::PATH,
                &relation,
                target_store,
                target_hook,
            )?;
        }

        for_each_relation_target_value(&value, |item| {
            validate_save_accepted_relation_value::<E>(&relation, target_store, item)
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
    validate_relation_primary_key_component_kind(target.scalar_target_key_kind)?;

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
        scalar_target_key_kind: target.scalar_target_key_kind.clone(),
    }))
}

fn target_store_for_relation<E>(
    db: &Db<E::Canister>,
    relation: &AcceptedSaveStrongRelationInfo,
    value: &Value,
) -> Result<StoreHandle, InternalError>
where
    E: EntityKind + EntityValue,
{
    db.with_store_registry(|registry| registry.try_get_store(relation.target.store_path()))
        .map_err(|err| {
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.field_name.as_str(),
                relation.target.path(),
                relation.target.store_path(),
                value,
                err,
            )
        })
}

fn validate_save_accepted_relation_value<E>(
    relation: &AcceptedSaveStrongRelationInfo,
    target_store: StoreHandle,
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

fn validate_target_accepted_scalar_primary_key<C>(
    source_path: &'static str,
    relation: &AcceptedSaveStrongRelationInfo,
    target_store: StoreHandle,
    target_hook: &EntityRuntimeHooks<C>,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    let accepted = target_store.with_schema_mut(|schema_store| {
        ensure_accepted_schema_snapshot(
            schema_store,
            relation.target.entity_tag(),
            target_hook.entity_path,
            target_hook.model,
        )
    })?;
    let primary_key_kinds = accepted.primary_key_field_kinds();
    validate_target_accepted_scalar_primary_key_kinds(
        source_path,
        relation.field_name.as_str(),
        relation.target.path(),
        &relation.scalar_target_key_kind,
        &primary_key_kinds,
    )
}

fn validate_target_accepted_scalar_primary_key_kinds(
    source_path: &str,
    field_name: &str,
    target_path: &str,
    scalar_relation_key_kind: &PersistedFieldKind,
    primary_key_kinds: &[&PersistedFieldKind],
) -> Result<(), InternalError> {
    let [accepted_key_kind] = primary_key_kinds else {
        return Err(InternalError::strong_relation_target_identity_mismatch(
            source_path,
            field_name,
            target_path,
            format!(
                "target accepted primary key is composite with {} components; relation fields require one scalar target key",
                primary_key_kinds.len()
            ),
        ));
    };

    if *accepted_key_kind != scalar_relation_key_kind {
        return Err(InternalError::strong_relation_target_identity_mismatch(
            source_path,
            field_name,
            target_path,
            format!(
                "target accepted primary-key kind {accepted_key_kind:?} does not match scalar relation key kind {scalar_relation_key_kind:?}"
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_target_accepted_scalar_primary_key_kinds;
    use crate::{db::schema::PersistedFieldKind, error::ErrorClass};

    #[test]
    fn save_relation_target_pk_guard_rejects_composite_target_authority() {
        let kinds = [&PersistedFieldKind::Nat64, &PersistedFieldKind::Ulid];

        let err = validate_target_accepted_scalar_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &PersistedFieldKind::Nat64,
            &kinds,
        )
        .expect_err("save relation guard must reject composite target PK authority");

        assert_eq!(err.class, ErrorClass::Internal);
        assert!(
            err.message.contains("composite"),
            "diagnostic should explain composite target PK rejection: {err:?}"
        );
    }

    #[test]
    fn save_relation_target_pk_guard_rejects_kind_drift() {
        let kinds = [&PersistedFieldKind::Nat128];

        let err = validate_target_accepted_scalar_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &PersistedFieldKind::Nat64,
            &kinds,
        )
        .expect_err("save relation guard must reject relation/target key-kind drift");

        assert_eq!(err.class, ErrorClass::Internal);
        assert!(
            err.message
                .contains("does not match scalar relation key kind"),
            "diagnostic should explain key-kind drift: {err:?}"
        );
    }
}
