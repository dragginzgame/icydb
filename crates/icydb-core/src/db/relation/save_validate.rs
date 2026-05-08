//! Module: relation::save_validate
//! Responsibility: validate save-time strong relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates strong-relation target validation to this module.

#[cfg(test)]
use crate::db::relation::{
    metadata::{StrongRelationInfo, StrongRelationMetadataError, strong_relations_for_model_iter},
    raw_relation_target_key_from_value,
};
use crate::{
    db::{
        Db,
        identity::EntityName,
        relation::for_each_relation_target_value,
        schema::{AcceptedRowDecodeContract, PersistedFieldKind, PersistedRelationStrength},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::EntityTag,
    value::Value,
};

// Save-time strong relation metadata projected from the accepted row contract.
// This is intentionally narrower than generated relation metadata: save
// validation only needs the source slot, source field name, and sealed target
// identity before it checks target-store membership.
struct AcceptedSaveStrongRelationInfo {
    field_index: usize,
    field_name: String,
    target: AcceptedSaveStrongRelationTargetIdentity,
}

struct AcceptedSaveStrongRelationTargetIdentity {
    path: String,
    entity_name: EntityName,
    entity_tag: EntityTag,
    store_path: String,
}

impl AcceptedSaveStrongRelationTargetIdentity {
    fn try_new(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
    ) -> Result<Self, InternalError> {
        let entity_name = EntityName::try_from_str(target_entity_name).map_err(|err| {
            InternalError::strong_relation_target_name_invalid(
                source_path,
                field_name,
                target_path,
                target_entity_name,
                err,
            )
        })?;

        Ok(Self {
            path: target_path.to_string(),
            entity_name,
            entity_tag: target_entity_tag,
            store_path: target_store_path.to_string(),
        })
    }

    fn validate_against_db<C>(
        &self,
        db: &Db<C>,
        source_path: &str,
        field_name: &str,
    ) -> Result<(), InternalError>
    where
        C: crate::traits::CanisterKind,
    {
        if !db.has_runtime_hooks() {
            return Ok(());
        }

        let hook = db
            .runtime_hook_for_entity_tag(self.entity_tag)
            .map_err(|err| {
                InternalError::strong_relation_target_identity_mismatch(
                    source_path,
                    field_name,
                    self.path.as_str(),
                    format!(
                        "target_entity_tag={} is not registered: {err}",
                        self.entity_tag.value()
                    ),
                )
            })?;

        if hook.entity_path != self.path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_entity_tag={} resolves to entity_path={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.entity_path,
                    self.path
                ),
            ));
        }

        if hook.model.name() != self.entity_name.as_str() {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_entity_tag={} resolves to entity_name={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.model.name(),
                    self.entity_name.as_str(),
                ),
            ));
        }

        if hook.store_path != self.store_path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
                format!(
                    "target_store_path={} does not match runtime store {} for target_entity_tag={}",
                    self.store_path,
                    hook.store_path,
                    self.entity_tag.value(),
                ),
            ));
        }

        Ok(())
    }
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

/// Validate strong relation references for one save candidate entity.
#[cfg(test)]
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
    let Some((target_path, target_entity_name, target_entity_tag, target_store_path, strength)) =
        accepted_relation_target_from_kind(kind)
    else {
        return Ok(None);
    };
    if strength != PersistedRelationStrength::Strong {
        return Ok(None);
    }

    Ok(Some(AcceptedSaveStrongRelationInfo {
        field_index,
        field_name: field_name.to_string(),
        target: AcceptedSaveStrongRelationTargetIdentity::try_new(
            source_path,
            field_name,
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
        )?,
    }))
}

fn accepted_relation_target_from_kind(
    kind: &PersistedFieldKind,
) -> Option<(&str, &str, EntityTag, &str, PersistedRelationStrength)> {
    const fn relation_target(
        kind: &PersistedFieldKind,
    ) -> Option<(&str, &str, EntityTag, &str, PersistedRelationStrength)> {
        let PersistedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            strength,
            ..
        } = kind
        else {
            return None;
        };

        Some((
            target_path.as_str(),
            target_entity_name.as_str(),
            *target_entity_tag,
            target_store_path.as_str(),
            *strength,
        ))
    }

    match kind {
        PersistedFieldKind::Relation { .. } => relation_target(kind),
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            relation_target(inner.as_ref())
        }
        _ => None,
    }
}

/// Validate one strong relation key against target-store membership.
#[cfg(test)]
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

fn validate_save_accepted_relation_value<E>(
    db: &Db<E::Canister>,
    relation: &AcceptedSaveStrongRelationInfo,
    value: &Value,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    let storage_key = crate::value::storage_key_from_runtime_value(value).map_err(|err| {
        InternalError::relation_target_raw_key_error(
            crate::db::relation::RelationTargetRawKeyError::StorageKeyEncode(err),
            E::PATH,
            relation.field_name.as_str(),
            relation.target.path.as_str(),
            value,
            "strong relation target key unsupported",
        )
    })?;
    let raw_key = crate::db::data::DataKey::raw_from_parts(relation.target.entity_tag, storage_key)
        .map_err(|err| {
            InternalError::relation_target_raw_key_error(
                crate::db::relation::RelationTargetRawKeyError::StorageKeyEncode(err),
                E::PATH,
                relation.field_name.as_str(),
                relation.target.path.as_str(),
                value,
                "strong relation target key unsupported",
            )
        })?;
    let target_store = db
        .with_store_registry(|registry| registry.try_get_store(relation.target.store_path.as_str()))
        .map_err(|err| {
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.field_name.as_str(),
                relation.target.path.as_str(),
                relation.target.store_path.as_str(),
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
            relation.target.path.as_str(),
            value,
        ))
    }
}

// Map static metadata lowering failures at the save validation boundary where
// the typed source entity path is still available for diagnostics.
#[cfg(test)]
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
