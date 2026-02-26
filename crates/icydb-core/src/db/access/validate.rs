use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
        contracts::{SchemaInfo, literal_matches_type},
        query::predicate::coercion::canonical_cmp,
    },
    model::{entity::EntityModel, index::IndexModel},
    traits::FieldValue,
    value::Value,
};
use std::ops::Bound;
use thiserror::Error as ThisError;

///
/// AccessPlanError
///
/// Access-path and key-shape validation failures.
///
#[derive(Debug, ThisError)]
pub enum AccessPlanError {
    /// Access plan references an index not declared on the entity.
    #[error("index '{index}' not found on entity")]
    IndexNotFound { index: IndexModel },

    /// Index prefix exceeds the number of indexed fields.
    #[error("index prefix length {prefix_len} exceeds index field count {field_len}")]
    IndexPrefixTooLong { prefix_len: usize, field_len: usize },

    /// Index prefix must include at least one value.
    #[error("index prefix must include at least one value")]
    IndexPrefixEmpty,

    /// Index prefix literal does not match indexed field type.
    #[error("index prefix value for field '{field}' is incompatible")]
    IndexPrefixValueMismatch { field: String },

    /// Primary key field exists but is not key-compatible.
    #[error("primary key field '{field}' is not key-compatible")]
    PrimaryKeyNotKeyable { field: String },

    /// Supplied key does not match the primary key type.
    #[error("key '{key:?}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Value },

    /// Key range has invalid ordering.
    #[error("key range start is greater than end")]
    InvalidKeyRange,
}

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
    ) -> Result<(), AccessPlanError>;

    /// Validate a key range and enforce representation-specific ordering rules.
    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), AccessPlanError>;
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
    ) -> Result<(), AccessPlanError> {
        validate_pk_key(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &K,
        end: &K,
    ) -> Result<(), AccessPlanError> {
        validate_pk_key(schema, model, start)?;
        validate_pk_key(schema, model, end)?;

        // Executor-boundary validation is defensive and must not reject
        // planner-level key-range semantics. Inverted ranges are allowed here
        // and execute as empty scans.

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
    ) -> Result<(), AccessPlanError> {
        validate_pk_value(schema, model, key)
    }

    fn validate_key_range(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        start: &Value,
        end: &Value,
    ) -> Result<(), AccessPlanError> {
        validate_pk_value(schema, model, start)?;
        validate_pk_value(schema, model, end)?;
        let ordering = canonical_cmp(start, end);
        if ordering == std::cmp::Ordering::Greater {
            return Err(AccessPlanError::InvalidKeyRange);
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
) -> Result<(), AccessPlanError> {
    access.validate(schema, model, adapter)
}

/// Validate executor-visible access paths.
///
/// This ensures keys, ranges, and index prefixes are schema-compatible.
pub(crate) fn validate_access_plan<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<K>,
) -> Result<(), AccessPlanError>
where
    K: FieldValue + Ord,
{
    validate_access_plan_with(schema, model, access, &GenericKeyAdapter)
}

/// Validate access paths that carry model-level key values.
pub(crate) fn validate_access_plan_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<Value>,
) -> Result<(), AccessPlanError> {
    validate_access_plan_with(schema, model, access, &ValueKeyAdapter)
}

/// Validate that a key matches the entity's primary key type.
fn validate_pk_key<K>(
    schema: &SchemaInfo,
    model: &EntityModel,
    key: &K,
) -> Result<(), AccessPlanError>
where
    K: FieldValue,
{
    let value = key.to_value();
    validate_pk_literal(schema, model, &value)
}

// Validate that a model-level key value matches the entity's primary key type.
fn validate_pk_value(
    schema: &SchemaInfo,
    model: &EntityModel,
    key: &Value,
) -> Result<(), AccessPlanError> {
    validate_pk_literal(schema, model, key)
}

// Validate that a primary-key literal matches the entity primary-key schema.
fn validate_pk_literal(
    schema: &SchemaInfo,
    model: &EntityModel,
    key: &Value,
) -> Result<(), AccessPlanError> {
    let field = model.primary_key.name;

    let field_type = schema
        .field(field)
        .ok_or_else(|| AccessPlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        })?;

    if !field_type.is_keyable() {
        return Err(AccessPlanError::PrimaryKeyNotKeyable {
            field: field.to_string(),
        });
    }

    if !literal_matches_type(key, field_type) {
        return Err(AccessPlanError::PrimaryKeyMismatch {
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
) -> Result<(), AccessPlanError> {
    if !model.indexes.contains(&index) {
        return Err(AccessPlanError::IndexNotFound { index: *index });
    }

    if values.is_empty() {
        return Err(AccessPlanError::IndexPrefixEmpty);
    }

    if values.len() > index.fields.len() {
        return Err(AccessPlanError::IndexPrefixTooLong {
            prefix_len: values.len(),
            field_len: index.fields.len(),
        });
    }

    for (field, value) in index.fields.iter().zip(values.iter()) {
        let field_type =
            schema
                .field(field)
                .ok_or_else(|| AccessPlanError::IndexPrefixValueMismatch {
                    field: field.to_string(),
                })?;

        if !literal_matches_type(value, field_type) {
            return Err(AccessPlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            });
        }
    }

    Ok(())
}

/// Validate that an index range path is valid for execution.
fn validate_index_range(
    schema: &SchemaInfo,
    model: &EntityModel,
    spec: &SemanticIndexRangeSpec,
) -> Result<(), AccessPlanError> {
    let index = spec.index();
    let prefix = spec.prefix_values();
    let lower = spec.lower();
    let upper = spec.upper();

    if !model.indexes.contains(&index) {
        return Err(AccessPlanError::IndexNotFound { index: *index });
    }

    if prefix.len() >= index.fields.len() {
        return Err(AccessPlanError::IndexPrefixTooLong {
            prefix_len: prefix.len(),
            field_len: index.fields.len().saturating_sub(1),
        });
    }

    let range_slot = prefix.len();
    if spec.field_slots().len() != prefix.len().saturating_add(1) {
        return Err(AccessPlanError::InvalidKeyRange);
    }
    for (expected_slot, actual_slot) in (0..=range_slot).zip(spec.field_slots().iter().copied()) {
        if actual_slot != expected_slot {
            return Err(AccessPlanError::InvalidKeyRange);
        }
    }

    for (field, value) in index.fields.iter().zip(prefix.iter()) {
        let field_type =
            schema
                .field(field)
                .ok_or_else(|| AccessPlanError::IndexPrefixValueMismatch {
                    field: field.to_string(),
                })?;

        if !literal_matches_type(value, field_type) {
            return Err(AccessPlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            });
        }
    }

    let range_field = index.fields[range_slot];
    validate_index_range_bound_value(schema, range_field, lower)?;
    validate_index_range_bound_value(schema, range_field, upper)?;

    let (
        Bound::Included(lower_value) | Bound::Excluded(lower_value),
        Bound::Included(upper_value) | Bound::Excluded(upper_value),
    ) = (lower, upper)
    else {
        return Ok(());
    };

    if canonical_cmp(lower_value, upper_value) == std::cmp::Ordering::Greater {
        return Err(AccessPlanError::InvalidKeyRange);
    }

    Ok(())
}

fn validate_index_range_bound_value(
    schema: &SchemaInfo,
    field: &'static str,
    bound: &Bound<Value>,
) -> Result<(), AccessPlanError> {
    let value = match bound {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => return Ok(()),
    };

    let field_type =
        schema
            .field(field)
            .ok_or_else(|| AccessPlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            })?;

    if literal_matches_type(value, field_type) {
        return Ok(());
    }

    Err(AccessPlanError::IndexPrefixValueMismatch {
        field: field.to_string(),
    })
}

impl<K> AccessPlan<K> {
    // Validate this access plan using adapter-specific key semantics.
    fn validate(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        adapter: &impl AccessPlanKeyAdapter<K>,
    ) -> Result<(), AccessPlanError> {
        match self {
            Self::Path(path) => path.validate(schema, model, adapter),
            Self::Union(children) | Self::Intersection(children) => {
                for child in children {
                    child.validate(schema, model, adapter)?;
                }

                Ok(())
            }
        }
    }
}

impl<K> AccessPath<K> {
    // Validate this concrete access path using adapter-specific key semantics.
    fn validate(
        &self,
        schema: &SchemaInfo,
        model: &EntityModel,
        adapter: &impl AccessPlanKeyAdapter<K>,
    ) -> Result<(), AccessPlanError> {
        match self {
            Self::ByKey(key) => adapter.validate_pk_key(schema, model, key),
            Self::ByKeys(keys) => {
                // Empty key lists are a valid no-op.
                if keys.is_empty() {
                    return Ok(());
                }
                for key in keys {
                    adapter.validate_pk_key(schema, model, key)?;
                }

                Ok(())
            }
            Self::KeyRange { start, end } => adapter.validate_key_range(schema, model, start, end),
            Self::IndexPrefix { index, values } => {
                validate_index_prefix(schema, model, index, values)
            }
            Self::IndexRange { spec } => validate_index_range(schema, model, spec),
            Self::FullScan => Ok(()),
        }
    }
}
