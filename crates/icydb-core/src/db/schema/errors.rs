//! Module: db::schema::errors
//! Responsibility: schema validation error taxonomy for runtime schema contracts.
//! Does not own: predicate AST or planning policy logic.
//! Boundary: error surface for schema construction and predicate-schema validation.

use crate::{
    db::predicate::{CoercionId, CompareOp},
    model::index::{IndexExpression, IndexModel},
};
use std::fmt;

/// Compact predicate operator identity for schema validation diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaValidationOperator {
    Compare(CompareOp),
    CompareField { op: CompareOp, right_field: String },
    IsEmpty,
    IsNotEmpty,
    TextContains,
    TextContainsCi,
}

impl SchemaValidationOperator {
    pub(crate) const fn compare(op: CompareOp) -> Self {
        Self::Compare(op)
    }

    pub(crate) fn compare_field(op: CompareOp, right_field: &str) -> Self {
        Self::CompareField {
            op,
            right_field: right_field.to_string(),
        }
    }
}

impl fmt::Display for SchemaValidationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compare(op) => write!(f, "{op:?}"),
            Self::CompareField { op, right_field } => {
                write!(f, "{op:?} against field '{right_field}'")
            }
            Self::IsEmpty => f.write_str("is_empty"),
            Self::IsNotEmpty => f.write_str("is_not_empty"),
            Self::TextContains => f.write_str("text_contains"),
            Self::TextContainsCi => f.write_str("text_contains_ci"),
        }
    }
}

/// Compact literal validation reason for schema validation diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaLiteralValidationReason {
    ExpectedList,
    ExpectedText,
    ExpectedScalar,
    LiteralTypeMismatch,
    ListElementTypeMismatch,
    EnumPathMismatch,
    UnknownEnumVariant,
    EnumBodyMismatch,
}

impl fmt::Display for SchemaLiteralValidationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedList => f.write_str("expected list literal"),
            Self::ExpectedText => f.write_str("expected text literal"),
            Self::ExpectedScalar => f.write_str("expected scalar literal"),
            Self::LiteralTypeMismatch => f.write_str("literal type does not match field type"),
            Self::ListElementTypeMismatch => {
                f.write_str("list literal does not match field element type")
            }
            Self::EnumPathMismatch => f.write_str("enum path does not match field enum type"),
            Self::UnknownEnumVariant => f.write_str("enum variant is not accepted for field type"),
            Self::EnumBodyMismatch => {
                f.write_str("enum payload does not match the accepted variant contract")
            }
        }
    }
}

/// Predicate/schema validation failures, including invalid model contracts.
#[derive(Debug, thiserror::Error)]
pub enum ValidateError {
    #[error("unknown field '{field}'")]
    UnknownField { field: String },

    #[error("field '{field}' is not queryable")]
    NonQueryableFieldType { field: String },

    #[error("duplicate field '{field}'")]
    DuplicateField { field: String },

    #[error("map predicates are unsupported for field '{field}'")]
    MapPredicateUnsupported { field: String },

    #[error("primary key '{field}' not present in entity fields")]
    InvalidPrimaryKey { field: String },

    #[error("primary key '{field}' has a non-keyable type")]
    InvalidPrimaryKeyType { field: String },

    #[error("index '{index}' references unknown field '{field}'")]
    IndexFieldUnknown {
        index: Box<IndexModel>,
        field: String,
    },

    #[error("index '{index}' references non-queryable field '{field}'")]
    IndexFieldNotQueryable {
        index: Box<IndexModel>,
        field: String,
    },

    #[error(
        "index '{index}' references map field '{field}'; map fields are not queryable in icydb 0.7"
    )]
    IndexFieldMapNotQueryable {
        index: Box<IndexModel>,
        field: String,
    },

    #[error("index '{index}' repeats field '{field}'")]
    IndexFieldDuplicate {
        index: Box<IndexModel>,
        field: String,
    },

    #[error("index '{index}' expression key item '{expression}' requires {expected}")]
    IndexExpressionFieldTypeInvalid {
        index: &'static str,
        expression: IndexExpression,
        expected: &'static str,
    },

    #[error("duplicate index name '{name}'")]
    DuplicateIndexName { name: String },

    #[error("index '{index}' predicate '{predicate}' has invalid SQL syntax")]
    InvalidIndexPredicateSyntax {
        index: Box<IndexModel>,
        predicate: &'static str,
    },

    #[error("index '{index}' predicate '{predicate}' is invalid for schema")]
    InvalidIndexPredicateSchema {
        index: Box<IndexModel>,
        predicate: &'static str,
    },

    #[error("operator {operator} is not valid for field '{field}'")]
    InvalidOperator {
        field: String,
        operator: SchemaValidationOperator,
    },

    #[error("coercion {coercion:?} is not valid for field '{field}'")]
    InvalidCoercion { field: String, coercion: CoercionId },

    #[error("invalid literal for field '{field}': {reason}")]
    InvalidLiteral {
        field: String,
        reason: SchemaLiteralValidationReason,
    },
}

impl ValidateError {
    pub(crate) fn invalid_operator(field: &str, operator: SchemaValidationOperator) -> Self {
        Self::InvalidOperator {
            field: field.to_string(),
            operator,
        }
    }

    pub(crate) fn invalid_literal(field: &str, reason: SchemaLiteralValidationReason) -> Self {
        Self::InvalidLiteral {
            field: field.to_string(),
            reason,
        }
    }
}
