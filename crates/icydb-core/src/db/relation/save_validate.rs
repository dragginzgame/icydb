//! Module: relation::save_validate
//! Responsibility: validate save-time relation targets against target
//! store existence before commit planning proceeds.
//! Does not own: reverse-index mutation planning or delete-time relation blocking.
//! Boundary: executor save preflight delegates relation target validation to this module.

use crate::{
    db::{
        Db,
        data::StructuralSlotReader,
        key_taxonomy::{CompositePrimaryKeyValue, PrimaryKeyComponent, PrimaryKeyValue},
        registry::{StoreHandle, StoreRelationSourceCapability, StoreRelationTargetCapability},
        relation::{
            AcceptedRelationTargetAuthority, AcceptedRelationTupleEdgeLocalComponent,
            accepted_relation_target_metadata_from_kind, accepted_relation_tuple_edge_descriptor,
            accepted_scalar_relation_target_descriptor, for_each_relation_target_value,
        },
        schema::{
            AcceptedFieldKind, AcceptedRowDecodeContract, OwnedAcceptedFieldDecodeContract,
            OwnedAcceptedRelationEdgeContract,
        },
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    traits::Path,
    value::Value,
};

// Save-time relation metadata projected from the accepted row contract.
// This is intentionally narrower than generated relation metadata: save
// validation only needs the source slot, source field name, and sealed target
// identity before it checks target-store membership.
struct AcceptedSaveRelationInfo {
    relation_name: String,
    local_components: Vec<AcceptedSaveRelationLocalComponent>,
    target: AcceptedRelationTargetAuthority,
}

struct AcceptedSaveRelationLocalComponent {
    index: usize,
    kind: AcceptedFieldKind,
}

impl AcceptedSaveRelationInfo {
    fn new(
        relation_name: impl Into<String>,
        local_components: Vec<AcceptedSaveRelationLocalComponent>,
        target: AcceptedRelationTargetAuthority,
    ) -> Self {
        Self {
            relation_name: relation_name.into(),
            local_components,
            target,
        }
    }

    fn scalar_relation_component(&self) -> Option<&AcceptedSaveRelationLocalComponent> {
        let [component] = self.local_components.as_slice() else {
            return None;
        };
        accepted_relation_target_metadata_from_kind(&component.kind).map(|_| component)
    }
}

impl AcceptedSaveRelationLocalComponent {
    const fn new(index: usize, kind: AcceptedFieldKind) -> Self {
        Self { index, kind }
    }

    fn from_field(index: usize, field: &OwnedAcceptedFieldDecodeContract) -> Self {
        Self::new(index, field.kind().clone())
    }
}

/// Validate relation references through accepted schema metadata.
pub(in crate::db) fn validate_save_relations_with_accepted_contract<E>(
    db: &Db<E::Canister>,
    entity: &E,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    validate_save_relations_from_relation_edges(
        db,
        E::PATH,
        E::Store::PATH,
        accepted_row_decode_contract,
        |index| {
            entity
                .get_value_by_index(index)
                .ok_or_else(InternalError::executor_invariant)
        },
    )
}

/// Validate relation references from one accepted structural after-image.
pub(in crate::db) fn validate_save_relations_for_structural_row<C>(
    db: &Db<C>,
    entity_path: &'static str,
    store_path: &'static str,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
    row: &StructuralSlotReader<'_>,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    validate_save_relations_from_relation_edges(
        db,
        entity_path,
        store_path,
        accepted_row_decode_contract,
        |index| row.required_cached_value(index).cloned(),
    )
}

fn validate_save_relations_from_relation_edges<C>(
    db: &Db<C>,
    entity_path: &'static str,
    store_path: &'static str,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
    mut value_at: impl FnMut(usize) -> Result<Value, InternalError>,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    for edge in accepted_row_decode_contract.relation_edges() {
        let Some(relation) =
            accepted_save_relation_from_edge(db, entity_path, accepted_row_decode_contract, edge)?
        else {
            continue;
        };

        validate_save_relation(db, entity_path, store_path, &relation, &mut value_at)?;
    }

    Ok(())
}

fn validate_save_relation<C>(
    db: &Db<C>,
    entity_path: &'static str,
    store_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
    value_at: &mut impl FnMut(usize) -> Result<Value, InternalError>,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    let target_store = target_store_for_relation(db, entity_path, relation)?;
    validate_relation_storage_capabilities(db, entity_path, store_path, relation, target_store)?;

    validate_save_relation_targets(entity_path, relation, target_store, value_at)
}

fn accepted_save_relation_from_edge<C>(
    db: &Db<C>,
    entity_path: &'static str,
    accepted_row_decode_contract: &AcceptedRowDecodeContract,
    edge: &OwnedAcceptedRelationEdgeContract,
) -> Result<Option<AcceptedSaveRelationInfo>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let local_fields = edge
        .local_field_slots()
        .iter()
        .map(|slot| {
            accepted_row_decode_contract
                .field_for_slot(*slot)
                .ok_or_else(InternalError::store_invariant)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(relation) =
        accepted_save_scalar_relation_from_edge(db, entity_path, edge, local_fields.as_slice())?
    {
        return Ok(Some(relation));
    }

    accepted_save_tuple_relation_from_edge(db, entity_path, edge, local_fields.as_slice())
}

fn accepted_save_scalar_relation_from_edge<C>(
    db: &Db<C>,
    entity_path: &'static str,
    edge: &OwnedAcceptedRelationEdgeContract,
    local_fields: &[&OwnedAcceptedFieldDecodeContract],
) -> Result<Option<AcceptedSaveRelationInfo>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    if let [field] = local_fields
        && let Some(descriptor) = accepted_scalar_relation_target_descriptor(
            db,
            entity_path,
            edge.name(),
            field.field_name(),
            field.kind(),
            Some(edge.target_path()),
        )?
    {
        return Ok(Some(AcceptedSaveRelationInfo::new(
            field.field_name(),
            vec![AcceptedSaveRelationLocalComponent::from_field(
                edge.local_field_slots()[0],
                field,
            )],
            descriptor.into_target_contract().into_target(),
        )));
    }

    Ok(None)
}

fn accepted_save_tuple_relation_from_edge<C>(
    db: &Db<C>,
    entity_path: &'static str,
    edge: &OwnedAcceptedRelationEdgeContract,
    local_fields: &[&OwnedAcceptedFieldDecodeContract],
) -> Result<Option<AcceptedSaveRelationInfo>, InternalError>
where
    C: crate::traits::CanisterKind,
{
    let local_component_facts = local_fields
        .iter()
        .map(|field| AcceptedRelationTupleEdgeLocalComponent::new(field.field_name(), field.kind()))
        .collect::<Vec<_>>();
    let tuple_descriptor = accepted_relation_tuple_edge_descriptor(
        db,
        entity_path,
        edge.name(),
        edge.target_path(),
        local_component_facts.as_slice(),
    )?;
    let mut local_components = Vec::with_capacity(local_fields.len());
    for (offset, field) in local_fields.iter().enumerate() {
        local_components.push(AcceptedSaveRelationLocalComponent::from_field(
            edge.local_field_slots()[offset],
            field,
        ));
    }

    Ok(Some(AcceptedSaveRelationInfo::new(
        edge.name(),
        local_components,
        tuple_descriptor.into_target_contract().into_target(),
    )))
}

fn target_store_for_relation<C>(
    db: &Db<C>,
    entity_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
) -> Result<StoreHandle, InternalError>
where
    C: crate::traits::CanisterKind,
{
    db.with_store_registry(|registry| registry.try_get_store(relation.target.store_path()))
        .map_err(|err| {
            InternalError::relation_target_store_missing(
                entity_path,
                relation.relation_name.as_str(),
                relation.target.path(),
                relation.target.store_path(),
                err,
            )
        })
}

fn validate_relation_storage_capabilities<C>(
    db: &Db<C>,
    entity_path: &'static str,
    store_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
    target_store: StoreHandle,
) -> Result<(), InternalError>
where
    C: crate::traits::CanisterKind,
{
    let source_store = db.with_store_registry(|registry| registry.try_get_store(store_path))?;
    let source_capability = source_store.storage_capabilities().relation_source();
    let target_capability = target_store.storage_capabilities().relation_target();
    if matches!(
        (source_capability, target_capability),
        (
            StoreRelationSourceCapability::DurableSource,
            StoreRelationTargetCapability::VolatileTarget,
        )
    ) {
        return Err(InternalError::relation_volatile_target_unsupported(
            entity_path,
            relation.relation_name.as_str(),
            relation.target.path(),
            store_path,
            relation.target.store_path(),
        ));
    }

    Ok(())
}

fn validate_save_relation_targets(
    entity_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
    target_store: StoreHandle,
    value_at: &mut impl FnMut(usize) -> Result<Value, InternalError>,
) -> Result<(), InternalError> {
    if let Some(component) = relation.scalar_relation_component() {
        let value = value_at(component.index)?;
        return for_each_relation_target_value(&value, |value| {
            let Some(component) = PrimaryKeyComponent::from_runtime_value(value) else {
                return Err(InternalError::relation_target_raw_key_error(
                    entity_path,
                    relation.relation_name.as_str(),
                    relation.target.path(),
                    value,
                    "relation target key unsupported",
                ));
            };
            validate_save_accepted_relation_key(
                entity_path,
                relation,
                target_store,
                &PrimaryKeyValue::Scalar(component),
                value,
            )
        });
    }

    let Some(key) = relation_target_key_from_components(entity_path, relation, value_at)? else {
        return Ok(());
    };
    validate_save_accepted_relation_key(
        entity_path,
        relation,
        target_store,
        &key,
        &key.as_runtime_value(),
    )
}

fn validate_save_accepted_relation_key(
    entity_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
    target_store: StoreHandle,
    key: &PrimaryKeyValue,
    diagnostic_value: &Value,
) -> Result<(), InternalError> {
    let raw_key = crate::db::data::DecodedDataStoreKey::new(relation.target.entity_tag(), key)
        .to_raw()
        .map_err(|_| InternalError::executor_unsupported())?;
    let target_exists = target_store
        .data_store()
        .with_borrow(|store| store.get(&raw_key).is_some());

    if target_exists {
        Ok(())
    } else {
        Err(InternalError::relation_target_missing(
            entity_path,
            relation.relation_name.as_str(),
            relation.target.path(),
            diagnostic_value,
        ))
    }
}

fn relation_target_key_from_components(
    entity_path: &'static str,
    relation: &AcceptedSaveRelationInfo,
    value_at: &mut impl FnMut(usize) -> Result<Value, InternalError>,
) -> Result<Option<PrimaryKeyValue>, InternalError> {
    let mut components = Vec::with_capacity(relation.local_components.len());
    let mut null_count = 0usize;

    for component in &relation.local_components {
        let value = value_at(component.index)?;
        if matches!(value, Value::Null) {
            null_count = null_count.saturating_add(1);
            continue;
        }
        let Some(primary_key_component) = PrimaryKeyComponent::from_runtime_value(&value) else {
            return Err(InternalError::relation_target_raw_key_error(
                entity_path,
                relation.relation_name.as_str(),
                relation.target.path(),
                &value,
                "relation target key unsupported",
            ));
        };
        components.push(primary_key_component);
    }

    if null_count == relation.local_components.len() {
        return Ok(None);
    }
    if null_count != 0 {
        return Err(InternalError::executor_unsupported());
    }

    match components.as_slice() {
        [component] => Ok(Some(PrimaryKeyValue::Scalar(*component))),
        _ => Ok(Some(PrimaryKeyValue::Composite(
            CompositePrimaryKeyValue::try_from_components(components.as_slice())
                .map_err(InternalError::relation_source_row_unsupported_key_kind)?,
        ))),
    }
}
