//! Module: relation::save_validate
//! Responsibility: validate save-time strong relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates strong-relation target validation to this module.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreHandle, StoreRelationSourceCapability, StoreRelationTargetCapability},
        relation::{
            AcceptedRelationTargetAuthority, AcceptedRelationTupleEdgeLocalComponent,
            accepted_relation_target_metadata_from_kind, accepted_relation_tuple_edge_descriptor,
            for_each_relation_target_value, validate_relation_primary_key_component_kind,
        },
        schema::{
            AcceptedRowDecodeContract, OwnedAcceptedFieldDecodeContract,
            OwnedAcceptedRelationEdgeContract, PersistedFieldKind, PersistedRelationStrength,
            ensure_accepted_schema_snapshot,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
    value::Value,
};

// Save-time strong relation metadata projected from the accepted row contract.
// This is intentionally narrower than generated relation metadata: save
// validation only needs the source slot, source field name, and sealed target
// identity before it checks target-store membership.
struct AcceptedSaveStrongRelationInfo {
    relation_name: String,
    local_components: Vec<AcceptedSaveStrongRelationLocalComponent>,
    target: AcceptedRelationTargetAuthority,
    target_primary_key_kinds: Vec<PersistedFieldKind>,
}

struct AcceptedSaveStrongRelationLocalComponent {
    index: usize,
    name: String,
    kind: PersistedFieldKind,
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
            .validate_against_db(db, source_path, self.relation_name.as_str())
    }

    fn scalar_relation_component(&self) -> Option<&AcceptedSaveStrongRelationLocalComponent> {
        let [component] = self.local_components.as_slice() else {
            return None;
        };
        accepted_relation_target_metadata_from_kind(&component.kind).map(|_| component)
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
    validate_save_strong_relations_from_relation_edges(db, entity, accepted_row_decode_contract)?;

    for slot in 0..accepted_row_decode_contract.required_slot_count() {
        if accepted_row_decode_contract
            .relation_edges()
            .iter()
            .any(|edge| edge.local_field_slots().contains(&slot))
        {
            continue;
        }
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

        let target_hook = relation.validate_target_identity(db, E::PATH)?;
        let target_store = target_store_for_relation::<E>(db, &relation)?;
        validate_strong_relation_storage_capabilities::<E>(db, &relation, target_store)?;
        if let Some(target_hook) = target_hook {
            validate_target_accepted_primary_key::<E::Canister>(
                E::PATH,
                &relation,
                target_store,
                target_hook,
            )?;
        }

        validate_save_relation_targets_for_entity::<E>(&relation, target_store, entity)?;
    }

    Ok(())
}

fn validate_save_strong_relations_from_relation_edges<E>(
    db: &Db<E::Canister>,
    entity: &E,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    for edge in accepted_row_decode_contract.relation_edges() {
        let Some(relation) =
            accepted_save_strong_relation_from_edge::<E>(db, accepted_row_decode_contract, edge)?
        else {
            continue;
        };

        let target_hook = relation.validate_target_identity(db, E::PATH)?;
        let target_store = target_store_for_relation::<E>(db, &relation)?;
        validate_strong_relation_storage_capabilities::<E>(db, &relation, target_store)?;
        if let Some(target_hook) = target_hook {
            validate_target_accepted_primary_key::<E::Canister>(
                E::PATH,
                &relation,
                target_store,
                target_hook,
            )?;
        }

        validate_save_relation_targets_for_entity::<E>(&relation, target_store, entity)?;
    }

    Ok(())
}

fn accepted_save_strong_relation_from_edge<E>(
    db: &Db<E::Canister>,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
    edge: &OwnedAcceptedRelationEdgeContract,
) -> Result<Option<AcceptedSaveStrongRelationInfo>, InternalError>
where
    E: EntityKind,
{
    let local_fields = edge
        .local_field_slots()
        .iter()
        .map(|slot| {
            accepted_row_decode_contract
                .field_for_slot(*slot)
                .ok_or_else(|| {
                    InternalError::store_invariant(format!(
                        "accepted relation edge '{}' local slot missing: source={} slot={}",
                        edge.name(),
                        E::PATH,
                        slot,
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(relation) =
        accepted_save_scalar_strong_relation_from_edge::<E>(edge, local_fields.as_slice())?
    {
        return Ok(Some(relation));
    }

    accepted_save_tuple_strong_relation_from_edge::<E>(db, edge, local_fields.as_slice())
}

fn accepted_save_scalar_strong_relation_from_edge<E>(
    edge: &OwnedAcceptedRelationEdgeContract,
    local_fields: &[&OwnedAcceptedFieldDecodeContract],
) -> Result<Option<AcceptedSaveStrongRelationInfo>, InternalError>
where
    E: EntityKind,
{
    if let [field] = local_fields
        && let Some(target) = accepted_relation_target_metadata_from_kind(field.kind())
    {
        if target.strength != PersistedRelationStrength::Strong {
            return Ok(None);
        }
        if target.target_path != edge.target_path() {
            return Err(InternalError::store_invariant(format!(
                "accepted relation edge '{}' target path mismatch: edge={} field={}",
                edge.name(),
                edge.target_path(),
                target.target_path,
            )));
        }
        validate_relation_primary_key_component_kind(target.scalar_target_key_kind)?;

        return Ok(Some(AcceptedSaveStrongRelationInfo {
            relation_name: field.field_name().to_string(),
            local_components: vec![AcceptedSaveStrongRelationLocalComponent {
                index: edge.local_field_slots()[0],
                name: field.field_name().to_string(),
                kind: field.kind().clone(),
            }],
            target: AcceptedRelationTargetAuthority::try_new(
                E::PATH,
                field.field_name(),
                target.target_path,
                target.target_entity_name,
                target.target_entity_tag,
                target.target_store_path,
            )?,
            target_primary_key_kinds: vec![target.scalar_target_key_kind.clone()],
        }));
    }

    Ok(None)
}

fn accepted_save_tuple_strong_relation_from_edge<E>(
    db: &Db<E::Canister>,
    edge: &OwnedAcceptedRelationEdgeContract,
    local_fields: &[&OwnedAcceptedFieldDecodeContract],
) -> Result<Option<AcceptedSaveStrongRelationInfo>, InternalError>
where
    E: EntityKind,
{
    let local_component_facts = local_fields
        .iter()
        .map(|field| AcceptedRelationTupleEdgeLocalComponent::new(field.field_name(), field.kind()))
        .collect::<Vec<_>>();
    let tuple_descriptor = accepted_relation_tuple_edge_descriptor(
        db,
        E::PATH,
        edge.name(),
        edge.target_path(),
        local_component_facts.as_slice(),
    )?;
    let target_primary_key_kinds = tuple_descriptor.primary_key_kinds().to_vec();

    let mut local_components = Vec::with_capacity(local_fields.len());
    for (offset, field) in local_fields.iter().enumerate() {
        local_components.push(AcceptedSaveStrongRelationLocalComponent {
            index: edge.local_field_slots()[offset],
            name: field.field_name().to_string(),
            kind: field.kind().clone(),
        });
    }

    Ok(Some(AcceptedSaveStrongRelationInfo {
        relation_name: edge.name().to_string(),
        local_components,
        target: tuple_descriptor.into_target_contract().into_target(),
        target_primary_key_kinds,
    }))
}

fn accepted_save_strong_relation_from_field(
    source_path: &str,
    field_index: usize,
    field_name: &str,
    kind: &PersistedFieldKind,
) -> Result<Option<AcceptedSaveStrongRelationInfo>, InternalError> {
    let Some(target) = accepted_relation_target_metadata_from_kind(kind) else {
        return Ok(None);
    };
    if target.strength != PersistedRelationStrength::Strong {
        return Ok(None);
    }
    validate_relation_primary_key_component_kind(target.scalar_target_key_kind)?;

    Ok(Some(AcceptedSaveStrongRelationInfo {
        relation_name: field_name.to_string(),
        local_components: vec![AcceptedSaveStrongRelationLocalComponent {
            index: field_index,
            name: field_name.to_string(),
            kind: kind.clone(),
        }],
        target: AcceptedRelationTargetAuthority::try_new(
            source_path,
            field_name,
            target.target_path,
            target.target_entity_name,
            target.target_entity_tag,
            target.target_store_path,
        )?,
        target_primary_key_kinds: vec![target.scalar_target_key_kind.clone()],
    }))
}

fn target_store_for_relation<E>(
    db: &Db<E::Canister>,
    relation: &AcceptedSaveStrongRelationInfo,
) -> Result<StoreHandle, InternalError>
where
    E: EntityKind + EntityValue,
{
    db.with_store_registry(|registry| registry.try_get_store(relation.target.store_path()))
        .map_err(|err| {
            InternalError::strong_relation_target_store_missing(
                E::PATH,
                relation.relation_name.as_str(),
                relation.target.path(),
                relation.target.store_path(),
                &Value::Null,
                err,
            )
        })
}

fn validate_strong_relation_storage_capabilities<E>(
    db: &Db<E::Canister>,
    relation: &AcceptedSaveStrongRelationInfo,
    target_store: StoreHandle,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    let source_store = db.with_store_registry(|registry| registry.try_get_store(E::Store::PATH))?;
    let source_capability = source_store.storage_capabilities().relation_source();
    let target_capability = target_store.storage_capabilities().relation_target();
    if matches!(
        (source_capability, target_capability),
        (
            StoreRelationSourceCapability::DurableSource,
            StoreRelationTargetCapability::VolatileTarget,
        )
    ) {
        return Err(InternalError::strong_relation_volatile_target_unsupported(
            E::PATH,
            relation.relation_name.as_str(),
            relation.target.path(),
            E::Store::PATH,
            relation.target.store_path(),
        ));
    }

    Ok(())
}

fn validate_save_relation_targets_for_entity<E>(
    relation: &AcceptedSaveStrongRelationInfo,
    target_store: StoreHandle,
    entity: &E,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    if let Some(component) = relation.scalar_relation_component() {
        let value = relation_component_value::<E>(entity, component)?;
        return for_each_relation_target_value(&value, |value| {
            let Some(component) = PrimaryKeyComponent::from_runtime_value(value) else {
                return Err(InternalError::relation_target_raw_key_error(
                    E::PATH,
                    relation.relation_name.as_str(),
                    relation.target.path(),
                    value,
                    "strong relation target key unsupported",
                ));
            };
            validate_save_accepted_relation_key::<E>(
                relation,
                target_store,
                &PrimaryKeyValue::Scalar(component),
                value,
            )
        });
    }

    let Some(key) = relation_target_key_from_entity_components::<E>(relation, entity)? else {
        return Ok(());
    };
    validate_save_accepted_relation_key::<E>(relation, target_store, &key, &key.as_runtime_value())
}

fn validate_save_accepted_relation_key<E>(
    relation: &AcceptedSaveStrongRelationInfo,
    target_store: StoreHandle,
    key: &PrimaryKeyValue,
    diagnostic_value: &Value,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    let raw_key = crate::db::data::DecodedDataStoreKey::new(relation.target.entity_tag(), key)
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
            relation.relation_name.as_str(),
            relation.target.path(),
            diagnostic_value,
        ))
    }
}

fn relation_component_value<E>(
    entity: &E,
    component: &AcceptedSaveStrongRelationLocalComponent,
) -> Result<Value, InternalError>
where
    E: EntityKind + EntityValue,
{
    entity.get_value_by_index(component.index).ok_or_else(|| {
        InternalError::executor_invariant(format!(
            "entity field missing: {} field={}",
            E::PATH,
            component.name
        ))
    })
}

fn relation_target_key_from_entity_components<E>(
    relation: &AcceptedSaveStrongRelationInfo,
    entity: &E,
) -> Result<Option<PrimaryKeyValue>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let mut components = Vec::with_capacity(relation.local_components.len());
    let mut null_count = 0usize;

    for component in &relation.local_components {
        let value = relation_component_value::<E>(entity, component)?;
        if matches!(value, Value::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        let Some(primary_key_component) = PrimaryKeyComponent::from_runtime_value(&value) else {
            return Err(InternalError::relation_target_raw_key_error(
                E::PATH,
                relation.relation_name.as_str(),
                relation.target.path(),
                &value,
                "strong relation target key unsupported",
            ));
        };
        components.push(primary_key_component);
    }

    if null_count == relation.local_components.len() {
        return Ok(None);
    }
    if null_count != 0 {
        return Err(InternalError::executor_unsupported(format!(
            "partial composite relation target tuple: source={} relation={} target={}",
            E::PATH,
            relation.relation_name,
            relation.target.path(),
        )));
    }

    match components.as_slice() {
        [component] => Ok(Some(PrimaryKeyValue::Scalar(*component))),
        _ => Ok(Some(PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(components.as_slice())
                .map_err(InternalError::relation_source_row_unsupported_key_kind)?,
        ))),
    }
}

fn validate_target_accepted_primary_key<C>(
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
    validate_target_accepted_primary_key_kinds(
        source_path,
        relation.relation_name.as_str(),
        relation.target.path(),
        relation.target_primary_key_kinds.as_slice(),
        &primary_key_kinds,
    )
}

fn validate_target_accepted_primary_key_kinds(
    source_path: &str,
    field_name: &str,
    target_path: &str,
    relation_key_kinds: &[PersistedFieldKind],
    primary_key_kinds: &[&PersistedFieldKind],
) -> Result<(), InternalError> {
    if primary_key_kinds.len() != relation_key_kinds.len() {
        return Err(InternalError::strong_relation_target_identity_mismatch(
            source_path,
            field_name,
            target_path,
            format!(
                "target accepted primary-key component count {} does not match relation component count {}",
                primary_key_kinds.len(),
                relation_key_kinds.len(),
            ),
        ));
    }

    for (accepted_key_kind, relation_key_kind) in primary_key_kinds.iter().zip(relation_key_kinds) {
        if *accepted_key_kind != relation_key_kind {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                target_path,
                format!(
                    "target accepted primary-key kind {accepted_key_kind:?} does not match relation key kind {relation_key_kind:?}"
                ),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_target_accepted_primary_key_kinds;
    use crate::{db::schema::PersistedFieldKind, error::ErrorClass};

    #[test]
    fn save_relation_target_pk_guard_rejects_composite_target_authority() {
        let kinds = [&PersistedFieldKind::Nat64, &PersistedFieldKind::Ulid];

        let err = validate_target_accepted_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &[PersistedFieldKind::Nat64],
            &kinds,
        )
        .expect_err("save relation guard must reject composite target PK authority");

        assert_eq!(err.class, ErrorClass::Internal);
        assert!(
            err.message.contains("component count"),
            "diagnostic should explain target PK arity rejection: {err:?}"
        );
    }

    #[test]
    fn save_relation_target_pk_guard_rejects_kind_drift() {
        let kinds = [&PersistedFieldKind::Nat128];

        let err = validate_target_accepted_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &[PersistedFieldKind::Nat64],
            &kinds,
        )
        .expect_err("save relation guard must reject relation/target key-kind drift");

        assert_eq!(err.class, ErrorClass::Internal);
        assert!(
            err.message.contains("does not match relation key kind"),
            "diagnostic should explain key-kind drift: {err:?}"
        );
    }

    #[test]
    fn save_relation_target_pk_guard_accepts_ordered_composite_key_kinds() {
        let kinds = [&PersistedFieldKind::Nat64, &PersistedFieldKind::Ulid];

        validate_target_accepted_primary_key_kinds(
            "Source",
            "author",
            "Target",
            &[PersistedFieldKind::Nat64, PersistedFieldKind::Ulid],
            &kinds,
        )
        .expect("matching ordered composite relation key kinds should validate");
    }
}
