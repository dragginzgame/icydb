//! Executor-facing validation for logical plans.
//!
//! This module validates that a `LogicalPlan` is *safe for execution* against a
//! concrete entity schema. It must not:
//!   - infer planner semantics
//!   - assert planner invariants
//!   - normalize or rewrite plans
//!
//! Planner correctness is enforced elsewhere. This layer exists solely to
//! protect the executor from malformed or schema-incompatible plans.

use super::{AccessPath, LogicalPlan, OrderSpec};
use crate::db::query::v2::predicate::validate::{FieldType, ScalarType};
use crate::{
    db::query::v2::predicate::{self, SchemaInfo},
    key::Key,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use thiserror::Error as ThisError;

/// Executor-visible validation failures for logical plans.
///
/// These errors indicate that a plan cannot be safely executed against the
/// current schema or entity definition. They are *not* planner bugs.
#[derive(Debug, ThisError)]
pub enum PlanError {
    /// Predicate failed schema-level validation.
    #[error("predicate validation failed: {0}")]
    PredicateInvalid(#[from] predicate::ValidateError),

    /// ORDER BY references an unknown field.
    #[error("unknown order field '{field}'")]
    UnknownOrderField { field: String },

    /// ORDER BY references a field that cannot be ordered.
    #[error("order field '{field}' is not orderable")]
    UnorderableField { field: String },

    /// Access plan references an index not declared on the entity.
    #[error("index '{index}' not found on entity")]
    IndexNotFound { index: IndexModel },

    /// Index prefix exceeds the number of indexed fields.
    #[error("index prefix length {prefix_len} exceeds index field count {field_len}")]
    IndexPrefixTooLong { prefix_len: usize, field_len: usize },

    /// Index prefix literal does not match indexed field type.
    #[error("index prefix value for field '{field}' is incompatible")]
    IndexPrefixValueMismatch { field: String },

    /// Primary key field exists but is not key-compatible.
    #[error("primary key field '{field}' is not key-compatible")]
    PrimaryKeyUnsupported { field: String },

    /// Supplied key does not match the primary key type.
    #[error("key '{key}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Key },

    /// Key range has invalid ordering.
    #[error("key range start is greater than end")]
    InvalidKeyRange,
}

/// Validate a logical plan against the concrete entity schema.
///
/// This is the *only* validation step required before execution.
pub fn validate_plan<E: EntityKind>(plan: &LogicalPlan) -> Result<(), PlanError> {
    let schema = SchemaInfo::from_entity::<E>()?;

    if let Some(predicate) = &plan.predicate {
        predicate::validate(&schema, predicate)?;
    }

    if let Some(order) = &plan.order {
        validate_order(&schema, order)?;
    }

    validate_access::<E>(&schema, &plan.access)?;

    Ok(())
}

impl LogicalPlan {
    /// Debug-only validation hook.
    ///
    /// Panics if the plan is executor-invalid. This must never run in release
    /// builds and must not attempt recovery or normalization.
    pub(crate) fn debug_validate<E: EntityKind>(&self) {
        if !cfg!(debug_assertions) {
            return;
        }

        if let Err(err) = validate_plan::<E>(self) {
            panic!("logical plan invariant violated: {err}");
        }
    }
}

/// Validate ORDER BY fields against the schema.
fn validate_order(schema: &SchemaInfo, order: &OrderSpec) -> Result<(), PlanError> {
    for (field, _) in &order.fields {
        let field_type = schema
            .field(field)
            .ok_or_else(|| PlanError::UnknownOrderField {
                field: field.clone(),
            })?;

        if !field_type.is_orderable() {
            return Err(PlanError::UnorderableField {
                field: field.clone(),
            });
        }
    }

    Ok(())
}

/// Validate executor-visible access paths.
///
/// This ensures keys, ranges, and index prefixes are schema-compatible.
fn validate_access<E: EntityKind>(
    schema: &SchemaInfo,
    access: &AccessPath,
) -> Result<(), PlanError> {
    match access {
        AccessPath::ByKey(key) => validate_pk_key::<E>(schema, key),
        AccessPath::ByKeys(keys) => {
            for key in keys {
                validate_pk_key::<E>(schema, key)?;
            }
            Ok(())
        }
        AccessPath::KeyRange { start, end } => {
            validate_pk_key::<E>(schema, start)?;
            validate_pk_key::<E>(schema, end)?;
            if start > end {
                return Err(PlanError::InvalidKeyRange);
            }
            Ok(())
        }
        AccessPath::IndexPrefix { index, values } => {
            validate_index_prefix::<E>(schema, index, values)
        }
        AccessPath::FullScan => Ok(()),
    }
}

/// Validate that a key matches the entity's primary key type.
fn validate_pk_key<E: EntityKind>(schema: &SchemaInfo, key: &Key) -> Result<(), PlanError> {
    let field = E::PRIMARY_KEY;

    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        })?;

    let expected =
        key_type_for_field(field_type).ok_or_else(|| PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        })?;

    if key_variant(key) != expected {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: *key,
        });
    }

    Ok(())
}

/// Validate that an index prefix is valid for execution.
fn validate_index_prefix<E: EntityKind>(
    schema: &SchemaInfo,
    index: &IndexModel,
    values: &[Value],
) -> Result<(), PlanError> {
    if !E::INDEXES.contains(&index) {
        return Err(PlanError::IndexNotFound { index: *index });
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

/// Internal classification of primary-key-compatible value variants.
///
/// This exists purely to decouple `Key` from `FieldType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyVariant {
    Account,
    Int,
    Principal,
    Subaccount,
    Timestamp,
    Uint,
    Ulid,
    Unit,
}

const fn key_variant(key: &Key) -> KeyVariant {
    match key {
        Key::Account(_) => KeyVariant::Account,
        Key::Int(_) => KeyVariant::Int,
        Key::Principal(_) => KeyVariant::Principal,
        Key::Subaccount(_) => KeyVariant::Subaccount,
        Key::Timestamp(_) => KeyVariant::Timestamp,
        Key::Uint(_) => KeyVariant::Uint,
        Key::Ulid(_) => KeyVariant::Ulid,
        Key::Unit => KeyVariant::Unit,
    }
}

/// Map scalar field types to compatible key variants.
///
/// Non-scalar and unsupported field types are intentionally excluded.
const fn key_type_for_field(field_type: &FieldType) -> Option<KeyVariant> {
    match field_type {
        FieldType::Scalar(ScalarType::Account) => Some(KeyVariant::Account),
        FieldType::Scalar(ScalarType::Int) => Some(KeyVariant::Int),
        FieldType::Scalar(ScalarType::Principal) => Some(KeyVariant::Principal),
        FieldType::Scalar(ScalarType::Subaccount) => Some(KeyVariant::Subaccount),
        FieldType::Scalar(ScalarType::Timestamp) => Some(KeyVariant::Timestamp),
        FieldType::Scalar(ScalarType::Uint) => Some(KeyVariant::Uint),
        FieldType::Scalar(ScalarType::Ulid) => Some(KeyVariant::Ulid),
        FieldType::Scalar(ScalarType::Unit) => Some(KeyVariant::Unit),
        _ => None,
    }
}
