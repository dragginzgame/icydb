use crate::{
    db::{
        executor::save::SaveExecutor,
        relation::{
            RelationTargetRawKeyError, StrongRelationTargetInfo, build_relation_target_raw_key,
            strong_relation_target_from_kind,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    /// Validate strong relation references against the target data stores.
    pub(super) fn validate_strong_relations(&self, entity: &E) -> Result<(), InternalError> {
        // Phase 1: identify strong relation fields and read their values.
        for field in E::MODEL.fields {
            let Some(relation) = strong_relation_target_from_kind(&field.kind) else {
                continue;
            };

            let value = entity.get_value(field.name).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
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
        relation: StrongRelationTargetInfo,
        value: &Value,
    ) -> Result<(), InternalError> {
        // Phase 1: normalize the key into a storage-compatible target raw key.
        let raw_key =
            build_relation_target_raw_key(relation.target_entity_name, value).map_err(|err| {
                match err {
                    RelationTargetRawKeyError::StorageKeyEncode(err) => InternalError::new(
                        ErrorClass::Unsupported,
                        ErrorOrigin::Executor,
                        format!(
                            "strong relation key not storage-compatible: source={} field={} target={} value={value:?} ({err})",
                            E::PATH,
                            field_name,
                            relation.target_path
                        ),
                    ),
                    RelationTargetRawKeyError::TargetEntityName(err) => InternalError::new(
                        ErrorClass::Internal,
                        ErrorOrigin::Executor,
                        format!(
                            "strong relation target name invalid: source={} field={} target={} name={} ({err})",
                            E::PATH,
                            field_name,
                            relation.target_path,
                            relation.target_entity_name
                        ),
                    ),
                }
            })?;

        // Phase 2: resolve the target store and confirm existence.
        let store = self
            .db
            .with_store_registry(|reg| reg.try_get_store(relation.target_store_path))
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
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
        let exists = store.with_data(|s| s.contains_key(&raw_key));
        if !exists {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
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
