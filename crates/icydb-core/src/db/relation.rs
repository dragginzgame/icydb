use crate::{
    db::{
        Db,
        identity::EntityName,
        store::{DataKey, DataStore, RawDataKey, StorageKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::{EntityFieldKind, RelationStrength},
    traits::{CanisterKind, EntityKind, EntityValue, Path, Storable},
    value::Value,
};
use std::{borrow::Cow, collections::BTreeSet, ops::Bound};

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub type StrongRelationDeleteValidateFn<C> =
    fn(&Db<C>, &str, &BTreeSet<RawDataKey>) -> Result<(), InternalError>;

///
/// StrongRelationDeleteValidator
///
/// Per-canister callback used by delete execution to enforce
/// strong relation constraints before entering the commit window.
///

pub struct StrongRelationDeleteValidator<C: CanisterKind> {
    validate: StrongRelationDeleteValidateFn<C>,
}

impl<C: CanisterKind> StrongRelationDeleteValidator<C> {
    /// Construct a delete-side strong relation validator callback.
    #[must_use]
    pub const fn new(validate: StrongRelationDeleteValidateFn<C>) -> Self {
        Self { validate }
    }

    pub(crate) fn validate(
        &self,
        db: &Db<C>,
        target_path: &str,
        deleted_target_keys: &BTreeSet<RawDataKey>,
    ) -> Result<(), InternalError> {
        (self.validate)(db, target_path, deleted_target_keys)
    }
}

///
/// StrongRelationInfo
///
/// Lightweight relation descriptor extracted from runtime field metadata.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct StrongRelationInfo {
    target_path: &'static str,
    target_entity_name: &'static str,
}

// Resolve a field-kind into strong relation metadata (if applicable).
const fn strong_relation_from_kind(kind: &EntityFieldKind) -> Option<StrongRelationInfo> {
    match kind {
        EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            strength: RelationStrength::Strong,
            ..
        }
        | EntityFieldKind::List(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            strength: RelationStrength::Strong,
            ..
        })
        | EntityFieldKind::Set(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationInfo {
            target_path,
            target_entity_name,
        }),
        _ => None,
    }
}

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

    // Fast path: skip scanning if this source model has no matching strong relation fields.
    if !S::MODEL.fields.iter().any(|field| {
        strong_relation_from_kind(&field.kind)
            .is_some_and(|relation| relation.target_path == target_path)
    }) {
        return Ok(());
    }

    let source_store = db.with_data(|reg| reg.try_get_store(S::Store::PATH))?;
    source_store.with_borrow(|store| {
        validate_source_store_rows::<S>(store, target_path, deleted_target_keys)
    })
}

// Scan source rows and reject deletes that would orphan a strong relation reference.
fn validate_source_store_rows<S>(
    source_store: &DataStore,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    let start = DataKey::lower_bound::<S>().to_raw()?;
    let end = DataKey::upper_bound::<S>().to_raw()?;

    // Phase 1: decode each source row and verify key consistency.
    for entry in source_store.range((Bound::Included(start), Bound::Included(end))) {
        let row_key = DataKey::try_from_raw(entry.key()).map_err(|err| {
            InternalError::new(
                ErrorClass::Corruption,
                ErrorOrigin::Store,
                format!(
                    "source row key decode failed during delete relation validation: source={} ({err})",
                    S::PATH
                ),
            )
        })?;
        let expected_key = row_key.try_key::<S>()?;

        let source = entry.value().try_decode::<S>().map_err(|err| {
            InternalError::new(
                ErrorClass::Corruption,
                ErrorOrigin::Serialize,
                format!(
                    "source row decode failed during delete relation validation: source={} ({err})",
                    S::PATH
                ),
            )
        })?;
        let source_key = source.id().key();
        if expected_key != source_key {
            return Err(InternalError::new(
                ErrorClass::Corruption,
                ErrorOrigin::Store,
                format!(
                    "source row key mismatch during delete relation validation: source={} expected={expected_key:?} actual={source_key:?}",
                    S::PATH
                ),
            ));
        }

        // Phase 2: validate all matching strong relation fields on this source row.
        for field in S::MODEL.fields {
            let Some(relation) = strong_relation_from_kind(&field.kind) else {
                continue;
            };
            if relation.target_path != target_path {
                continue;
            }

            let value = source.get_value(field.name).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "entity field missing during delete relation validation: source={} field={}",
                        S::PATH,
                        field.name
                    ),
                )
            })?;

            validate_relation_value_not_deleted::<S>(
                field.name,
                relation,
                &value,
                source_key,
                deleted_target_keys,
            )?;
        }
    }

    Ok(())
}

// Validate a relation-typed value against the pending deleted target key set.
fn validate_relation_value_not_deleted<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
    source_key: S::Key,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    match value {
        Value::List(items) => {
            for item in items {
                // Optional relation list entries may be explicit null values.
                if matches!(item, Value::Null) {
                    continue;
                }
                validate_relation_item_not_deleted::<S>(
                    field_name,
                    relation,
                    item,
                    source_key,
                    deleted_target_keys,
                )?;
            }
        }
        Value::Null => {}
        _ => {
            validate_relation_item_not_deleted::<S>(
                field_name,
                relation,
                value,
                source_key,
                deleted_target_keys,
            )?;
        }
    }

    Ok(())
}

// Validate a single scalar relation item against pending delete keys.
fn validate_relation_item_not_deleted<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
    source_key: S::Key,
    deleted_target_keys: &BTreeSet<RawDataKey>,
) -> Result<(), InternalError>
where
    S: EntityKind + EntityValue,
{
    let raw_target_key = raw_relation_target_key::<S>(field_name, relation, value)?;
    if deleted_target_keys.contains(&raw_target_key) {
        return Err(InternalError::new(
            ErrorClass::Conflict,
            ErrorOrigin::Executor,
            format!(
                "delete blocked by strong relation: source={} field={} source_id={source_key:?} target={} key={value:?}",
                S::PATH,
                field_name,
                relation.target_path
            ),
        ));
    }

    Ok(())
}

// Convert a relation value to its target raw data key representation.
fn raw_relation_target_key<S>(
    field_name: &str,
    relation: StrongRelationInfo,
    value: &Value,
) -> Result<RawDataKey, InternalError>
where
    S: EntityKind + EntityValue,
{
    let storage_key = StorageKey::try_from_value(value).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Executor,
            format!(
                "strong relation key not storage-compatible during delete relation validation: source={} field={} target={} value={value:?} ({err})",
                S::PATH,
                field_name,
                relation.target_path
            ),
        )
    })?;
    let entity_name = EntityName::try_from_str(relation.target_entity_name).map_err(|err| {
        InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Executor,
            format!(
                "strong relation target entity invalid during delete relation validation: source={} field={} target={} name={} ({err})",
                S::PATH,
                field_name,
                relation.target_path,
                relation.target_entity_name
            ),
        )
    })?;

    let entity_bytes = entity_name.to_bytes();
    let key_bytes = storage_key.to_bytes()?;
    let mut raw_bytes = [0u8; DataKey::STORED_SIZE_USIZE];
    raw_bytes[..EntityName::STORED_SIZE_USIZE].copy_from_slice(&entity_bytes);
    raw_bytes[EntityName::STORED_SIZE_USIZE..].copy_from_slice(&key_bytes);

    Ok(RawDataKey::from_bytes(Cow::Borrowed(raw_bytes.as_slice())))
}
