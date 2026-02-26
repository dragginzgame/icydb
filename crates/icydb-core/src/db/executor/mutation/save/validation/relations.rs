use crate::{
    db::{
        executor::mutation::save::SaveExecutor,
        relation::{
            StrongRelationTargetInfo, build_relation_target_raw_key,
            for_each_relation_target_value, incompatible_store_error,
            strong_relation_target_from_kind, target_key_mismatch_error,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E: EntityKind + EntityValue> SaveExecutor<E> {
    /// Validate strong relation references against the target data stores.
    pub(in crate::db::executor) fn validate_strong_relations(
        &self,
        entity: &E,
    ) -> Result<(), InternalError> {
        // Phase 1: identify strong relation fields and read their values.
        for (field_index, field) in E::MODEL.fields.iter().enumerate() {
            let Some(relation) = strong_relation_target_from_kind(&field.kind) else {
                continue;
            };

            let value = entity.get_value_by_index(field_index).ok_or_else(|| {
                InternalError::executor_invariant(format!(
                    "entity field missing: {} field={}",
                    E::PATH,
                    field.name
                ))
            })?;

            // Phase 2: validate each referenced key.
            for_each_relation_target_value(&value, |item| {
                // Collection enforcement is aggregate: every referenced key must exist.
                self.validate_strong_relation_value(field.name, relation, item)
            })?;
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
                InternalError::relation_target_raw_key_error(
                    err,
                    E::PATH,
                    field_name,
                    relation.target_path,
                    relation.target_entity_name,
                    value,
                    "strong relation key not storage-compatible",
                    "strong relation target name invalid",
                )
            })?;

        // Phase 2: resolve the target store and confirm existence.
        let store = self
            .db
            .with_store_registry(|reg| reg.try_get_store(relation.target_store_path))
            .map_err(|err| {
                incompatible_store_error(
                    E::PATH,
                    field_name,
                    relation.target_path,
                    relation.target_store_path,
                    value,
                    err,
                )
            })?;
        let exists = store.with_data(|s| s.contains_key(&raw_key));
        if !exists {
            return Err(target_key_mismatch_error(
                E::PATH,
                field_name,
                relation.target_path,
                value,
            ));
        }

        Ok(())
    }
}
