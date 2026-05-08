//! Module: access::validate
//! Responsibility: schema-aware access-plan shape and key compatibility validation.
//! Does not own: access-path lowering or runtime scan semantics.
//! Boundary: validation boundary before lowering/execution.

#[cfg(test)]
use crate::error::InternalError;
use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
        schema::{SchemaInfo, literal_matches_type},
    },
    model::{entity::EntityModel, index::IndexModel},
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

impl AccessPlanError {
    /// Map access-validation failures into query-boundary runtime invariants.
    #[cfg(test)]
    pub(crate) fn into_internal_error(self) -> InternalError {
        InternalError::query_invariant(self.to_string())
    }
}

/// Validate model-level access paths that carry `Value` keys.
pub(crate) fn validate_access_structure_model(
    schema: &SchemaInfo,
    model: &EntityModel,
    access: &AccessPlan<Value>,
) -> Result<(), AccessPlanError> {
    access.validate(schema, model)
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

    if values.len() > index.fields().len() {
        return Err(AccessPlanError::IndexPrefixTooLong {
            prefix_len: values.len(),
            field_len: index.fields().len(),
        });
    }

    for (field, value) in index.fields().iter().zip(values.iter()) {
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

/// Validate that an index multi-lookup path is valid for execution.
fn validate_index_multi_lookup(
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

    let Some(field) = index.fields().first() else {
        return Err(AccessPlanError::IndexPrefixTooLong {
            prefix_len: 1,
            field_len: 0,
        });
    };
    let field_type =
        schema
            .field(field)
            .ok_or_else(|| AccessPlanError::IndexPrefixValueMismatch {
                field: field.to_string(),
            })?;

    for value in values {
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

    if prefix.len() >= index.fields().len() {
        return Err(AccessPlanError::IndexPrefixTooLong {
            prefix_len: prefix.len(),
            field_len: index.fields().len().saturating_sub(1),
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

    for (field, value) in index.fields().iter().zip(prefix.iter()) {
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

    let range_field = index.fields()[range_slot];
    validate_index_range_bound_value(schema, range_field, lower)?;
    validate_index_range_bound_value(schema, range_field, upper)?;

    let (
        Bound::Included(lower_value) | Bound::Excluded(lower_value),
        Bound::Included(upper_value) | Bound::Excluded(upper_value),
    ) = (lower, upper)
    else {
        return Ok(());
    };

    if Value::canonical_cmp(lower_value, upper_value) == std::cmp::Ordering::Greater {
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

// Validate that primary-key range endpoints match schema and preserve canonical order.
fn validate_pk_range(
    schema: &SchemaInfo,
    model: &EntityModel,
    start: &Value,
    end: &Value,
) -> Result<(), AccessPlanError> {
    validate_pk_literal(schema, model, start)?;
    validate_pk_literal(schema, model, end)?;
    let ordering = Value::canonical_cmp(start, end);
    if ordering == std::cmp::Ordering::Greater {
        return Err(AccessPlanError::InvalidKeyRange);
    }

    Ok(())
}

impl AccessPlan<Value> {
    // Validate this access plan with model-level `Value` key semantics.
    fn validate(&self, schema: &SchemaInfo, model: &EntityModel) -> Result<(), AccessPlanError> {
        match self {
            Self::Path(path) => path.validate(schema, model),
            Self::Union(children) | Self::Intersection(children) => {
                for child in children {
                    child.validate(schema, model)?;
                }

                Ok(())
            }
        }
    }
}

impl AccessPath<Value> {
    // Validate this concrete value-keyed access path.
    fn validate(&self, schema: &SchemaInfo, model: &EntityModel) -> Result<(), AccessPlanError> {
        match self {
            Self::ByKey(key) => validate_pk_literal(schema, model, key),
            Self::ByKeys(keys) => {
                // Empty key lists are a valid no-op.
                if keys.is_empty() {
                    return Ok(());
                }
                for key in keys {
                    validate_pk_literal(schema, model, key)?;
                }

                Ok(())
            }
            Self::KeyRange { start, end } => validate_pk_range(schema, model, start, end),
            Self::IndexPrefix { index, values } => {
                validate_index_prefix(schema, model, index, values)
            }
            Self::IndexMultiLookup { index, values } => {
                validate_index_multi_lookup(schema, model, index, values)
            }
            Self::IndexRange { spec } => validate_index_range(schema, model, spec),
            Self::FullScan => Ok(()),
        }
    }
}
