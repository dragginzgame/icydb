use crate::{
    db::{
        executor::save::SaveExecutor,
        identity::EntityName,
        store::{DataKey, RawDataKey, StorageKey},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::field::{EntityFieldKind, RelationStrength},
    traits::{EntityKind, EntityValue, Storable},
    value::Value,
};
use std::borrow::Cow;

///
/// StrongRelationInfo
///
/// Lightweight descriptor for strong relation validation.
///

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct StrongRelationInfo {
    target_path: &'static str,
    target_entity_name: &'static str,
    target_store_path: &'static str,
}

// Resolve a field-kind into strong relation metadata (if applicable).
const fn strong_relation_from_kind(kind: &EntityFieldKind) -> Option<StrongRelationInfo> {
    match kind {
        EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }
        | EntityFieldKind::List(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        })
        | EntityFieldKind::Set(EntityFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength: RelationStrength::Strong,
            ..
        }) => Some(StrongRelationInfo {
            target_path,
            target_entity_name,
            target_store_path,
        }),
        _ => {
            // NOTE: Only strong Ref and collection (List/Set) Ref fields participate in save-time RI.
            None
        }
    }
}

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    /// Validate strong relation references against the target data stores.
    pub(super) fn validate_strong_relations(&self, entity: &E) -> Result<(), InternalError> {
        // Phase 1: identify strong relation fields and read their values.
        for field in E::MODEL.fields {
            let Some(relation) = strong_relation_from_kind(&field.kind) else {
                continue;
            };

            let value = entity.get_value(field.name).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!("entity field missing: {} field={}", E::PATH, field.name),
                )
            })?;

            // Phase 2: validate each referenced key.
            match &value {
                Value::List(items) => {
                    // Collection enforcement is aggregate: every referenced key must exist.
                    // NOTE: relation List/Set shapes are represented as Value::List at runtime.
                    for item in items {
                        // NOTE: Optional list entries are allowed; skip explicit None values.
                        if matches!(item, Value::Null) {
                            continue;
                        }
                        self.validate_strong_relation_value(field.name, relation, item)?;
                    }
                }
                Value::Null => {
                    // NOTE: Optional strong relations may be unset; None does not trigger RI.
                }
                _ => {
                    self.validate_strong_relation_value(field.name, relation, &value)?;
                }
            }
        }

        Ok(())
    }

    /// Validate a single strong relation key against the target store.
    fn validate_strong_relation_value(
        &self,
        field_name: &str,
        relation: StrongRelationInfo,
        value: &Value,
    ) -> Result<(), InternalError> {
        // Phase 1: normalize the key into a storage-compatible form.
        let storage_key = StorageKey::try_from_value(value).map_err(|err| {
            InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "strong relation key not storage-compatible: source={} field={} target={} value={value:?} ({err})",
                    E::PATH,
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
                    "strong relation target name invalid: source={} field={} target={} name={} ({err})",
                    E::PATH,
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
        let raw_key = RawDataKey::from_bytes(Cow::Borrowed(raw_bytes.as_slice()));

        // Phase 2: resolve the target store and confirm existence.
        let store = self
            .db
            .with_data(|reg| reg.try_get_store(relation.target_store_path))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Executor,
                    format!(
                        "strong relation target store missing: source={} field={} target={} store={} key={value:?} ({err})",
                        E::PATH,
                        field_name,
                        relation.target_path,
                        relation.target_store_path
                    ),
                )
            })?;
        let exists = store.with_borrow(|s| s.contains_key(&raw_key));
        if !exists {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Executor,
                format!(
                    "strong relation missing: source={} field={} target={} key={value:?}",
                    E::PATH,
                    field_name,
                    relation.target_path
                ),
            ));
        }

        Ok(())
    }
}
