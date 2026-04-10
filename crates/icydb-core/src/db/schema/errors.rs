//! Module: db::schema::errors
//! Responsibility: schema validation error taxonomy for runtime schema contracts.
//! Does not own: predicate AST or planning policy logic.
//! Boundary: error surface for schema construction and predicate-schema validation.

use crate::{
    db::{
        identity::{EntityNameError, IndexNameError},
        predicate::{CoercionId, UnsupportedQueryFeature},
    },
    model::index::{IndexExpression, IndexModel},
};
use std::fmt;

/// Predicate/schema validation failures, including invalid model contracts.
#[derive(Debug, thiserror::Error)]
pub enum ValidateError {
    #[error("invalid entity name '{name}': {source}")]
    InvalidEntityName {
        name: String,
        #[source]
        source: EntityNameError,
    },

    #[error("invalid index name for '{index}': {source}")]
    InvalidIndexName {
        index: Box<IndexModel>,
        #[source]
        source: IndexNameError,
    },

    #[error("unknown field '{field}'")]
    UnknownField { field: String },

    #[error("field '{field}' is not queryable")]
    NonQueryableFieldType { field: String },

    #[error("duplicate field '{field}'")]
    DuplicateField { field: String },

    #[error("{0}")]
    UnsupportedQueryFeature(#[from] UnsupportedQueryFeature),

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

    #[error("operator {op} is not valid for field '{field}'")]
    InvalidOperator { field: String, op: String },

    #[error("coercion {coercion:?} is not valid for field '{field}'")]
    InvalidCoercion { field: String, coercion: CoercionId },

    #[error("invalid literal for field '{field}': {message}")]
    InvalidLiteral { field: String, message: String },
}

impl ValidateError {
    pub(crate) fn invalid_operator(field: &str, op: impl fmt::Display) -> Self {
        Self::InvalidOperator {
            field: field.to_string(),
            op: op.to_string(),
        }
    }

    pub(crate) fn invalid_literal(field: &str, msg: &str) -> Self {
        Self::InvalidLiteral {
            field: field.to_string(),
            message: msg.to_string(),
        }
    }
}
