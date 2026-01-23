use crate::{
    db::query::v2::predicate::{self, SchemaInfo},
    key::Key,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use thiserror::Error as ThisError;

use super::{AccessPath, LogicalPlan, OrderSpec};
use crate::db::query::v2::predicate::validate::{FieldType, ScalarType};

#[derive(Debug, ThisError)]
pub enum PlanError {
    #[error("predicate validation failed: {0}")]
    PredicateInvalid(#[from] predicate::ValidateError),

    #[error("unknown order field '{field}'")]
    UnknownOrderField { field: String },

    #[error("order field '{field}' is not orderable")]
    UnorderableField { field: String },

    #[error("index '{index}' not found on entity")]
    IndexNotFound { index: IndexModel },

    #[error("index prefix length {prefix_len} exceeds index field count {field_len}")]
    IndexPrefixTooLong { prefix_len: usize, field_len: usize },

    #[error("index prefix value for field '{field}' is incompatible")]
    IndexPrefixValueMismatch { field: String },

    #[error("primary key field '{field}' is not key-compatible")]
    PrimaryKeyUnsupported { field: String },

    #[error("key '{key}' is incompatible with primary key '{field}'")]
    PrimaryKeyMismatch { field: String, key: Key },

    #[error("key range start is greater than end")]
    InvalidKeyRange,
}

#[must_use]
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

fn validate_pk_key<E: EntityKind>(schema: &SchemaInfo, key: &Key) -> Result<(), PlanError> {
    let field = E::PRIMARY_KEY;
    let field_type = schema
        .field(field)
        .ok_or_else(|| PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        })?;

    let Some(expected) = key_type_for_field(field_type) else {
        return Err(PlanError::PrimaryKeyUnsupported {
            field: field.to_string(),
        });
    };

    if key_variant(key) != expected {
        return Err(PlanError::PrimaryKeyMismatch {
            field: field.to_string(),
            key: *key,
        });
    }

    Ok(())
}

fn validate_index_prefix<E: EntityKind>(
    schema: &SchemaInfo,
    index: &IndexModel,
    values: &[Value],
) -> Result<(), PlanError> {
    if !E::INDEXES.iter().any(|idx| *idx == index) {
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

fn key_variant(key: &Key) -> KeyVariant {
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

fn key_type_for_field(field_type: &FieldType) -> Option<KeyVariant> {
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
