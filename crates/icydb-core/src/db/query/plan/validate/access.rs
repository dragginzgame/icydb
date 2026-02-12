use crate::{
    db::query::{
        plan::{AccessPath, AccessPlan},
        predicate::{self, SchemaInfo, coercion::canonical_cmp},
    },
    model::{entity::EntityModel, index::IndexModel},
    traits::FieldValue,
    value::Value,
};

use crate::db::query::plan::validate::PlanError;

///
/// AccessPlanKeyAdapter
/// Adapter for key validation and ordering across access-plan representations.
///

trait AccessPlanKeyAdapter<K> {
    /// Validate a key against the entity's primary key type.
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &K,
    ) -> Result<(), PlanError>;

    /// Validate a key range and enforce representation-specific ordering rules.
    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), PlanError>;
}

///
/// GenericKeyAdapater
/// Adapter for typed key plans (FieldValue + Ord)
///

struct GenericKeyAdapter;

impl<K> AccessPlanKeyAdapter<K> for GenericKeyAdapter
where
    K: FieldValue + Ord,
{
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &K,
    ) -> Result<(), PlanError> {
        validate_pk_key(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), PlanError> {
        validate_pk_key(schema, model, start)?;
        validate_pk_key(schema, model, end)?;
        if start > end {
            return Err(PlanError::InvalidKeyRange);
        }

        Ok(())
    }
}

///
/// ValueKeyAdapter
/// Adapter for model-level Value plans (partial ordering).
///

struct ValueKeyAdapter;

impl AccessPlanKeyAdapter<Value> for ValueKeyAdapter {
    fn validate_pk_key(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        key: &Value,
    ) -> Result<(), PlanError> {
        validate_pk_value(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &Value,
        end: &Value,
    ) -> Result<(), PlanError> {
        validate_pk_value(schema, model, start)?;
        validate_pk_value(schema, model, end)?;
        let ordering = canonical_cmp(start, end);
        if ordering == std::cmp::Ordering::Greater {
            return Err(PlanError::InvalidKeyRange);
        }

        Ok(())
    }
}

// Validate access plans by delegating key checks to the adapter.
fn validate_access_plan_with<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<K>,
    adapter: &impl AccessPlanKeyAdapter<K>,
) -> Result<(), PlanError> {
    match access {
        AccessPlan::Path(path) => validate_access_path_with(schema, model, path, adapter),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            for child in children {
                validate_access_plan_with(schema, model, child, adapter)?;
            }

            Ok(())
        }
    }
}

// Validate access paths using representation-specific key semantics.
fn validate_access_path_with<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPath<K>,
    adapter: &impl AccessPlanKeyAdapter<K>,
) -> Result<(), PlanError> {
    match access {
        AccessPath::ByKey(key) => adapter.validate_pk_key(schema, model, key),
        AccessPath::ByKeys(keys) => {
            // Empty key lists are a valid no-op.
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                adapter.validate_pk_key(schema, model, key)?;
            }

            Ok(())
        }
        AccessPath::KeyRange { start, end } => {
            adapter.validate_key_range(schema, model, start, end)
        }
        AccessPath::IndexPrefix { index, values } => {
            validate_index_prefix(schema, model, index, values)
        }
        AccessPath::FullScan => Ok(()),
    }
}

/// Validate executor-visible access paths.
///
/// This ensures keys, ranges, and index prefixes are schema-compatible.
pub fn validate_access_plan<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<K>,
) -> Result<(), PlanError>
where
    K: FieldValue + Ord,
{
    validate_access_plan_with(schema, model, access, &GenericKeyAdapter)
}

/// Validate access paths that carry model-level key values.
pub fn validate_access_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<Value>,
) -> Result<(), PlanError> {
    validate_access_plan_with(schema, model, access, &ValueKeyAdapter)
}

/// Validate that a key matches the entity's primary key type.
fn validate_pk_key<K>(schema: &SchemaInfo, model: &EntityModel, key: &K) -> Result<(), PlanError>
where
    K: FieldValue,
{
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        });
    }

    let value = key.to_value();
    if !predicate::validate::literal_matches_type(&value, field_type) {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: value,
        });
    }

    Ok(())
}

// Validate that a model-level key value matches the entity's primary key type.
fn validate_pk_value(
    schema: &SchemaInfo,
    model: &EntityModel,
    key: &Value,
) -> Result<(), PlanError> {
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(PlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        });
    }

    if !predicate::validate::literal_matches_type(key, field_type) {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: key.clone(),
        });
    }

    Ok(())
}

/// Validate that an index prefix is valid for execution.
fn validate_index_prefix(
    schema: &SchemaInfo,
    model: &EntityModel,
    index: &IndexModel,
    values: &[Value],
) -> Result<(), PlanError> {
    if !model.indexes.contains(&index) {
        return Err(PlanError::IndexNotFound { index: *index });
    }

    if values.is_empty() {
        return Err(PlanError::IndexPrefixEmpty);
    }

    if values.len() > index.fields.len() {
        return Err(PlanError::IndexPrefixTooLong {
            prefix_len: values.len(),
            field_len: index.fields.len(),
        });
    }

    for (field, value) in index.fields.iter().zip(values.iter()) {
        let field_type =
            schema
                .field(field)
                .ok_or_else(|| PlanError::IndexPrefixValueMismatch {
                    field: field.to_string(),
                })?;

        if !predicate::validate::literal_matches_type(value, field_type) {
            return Err(PlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            });
        }
    }

    Ok(())
}
