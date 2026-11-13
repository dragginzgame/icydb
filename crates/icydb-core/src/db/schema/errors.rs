//! Module: db::schema::errors
//! Responsibility: schema validation error taxonomy for runtime schema contracts.
//! Does not own: predicate AST or planning policy logic.
//! Boundary: error surface for schema construction and predicate-schema validation.

use crate::db::predicate::{CoercionId, CompareOp};
use std::fmt;

/// Compact predicate operator identity for schema validation diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaValidationOperator {
    /// Compares a field with a literal operand.
    Compare(CompareOp),
    /// Compare against another field rather than a literal.
    CompareField {
        /// Comparison operator applied to the two operands.
        op: CompareOp,
        /// Name of the comparison's right-hand field.
        right_field: String,
    },
    /// Matches an empty supported collection or text value.
    IsEmpty,
    /// Matches a nonempty supported collection or text value.
    IsNotEmpty,
    /// Matches a case-sensitive text substring.
    TextContains,
    /// Matches a substring after the canonical case-insensitive fold.
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
    /// The operator requires a list literal.
    ExpectedList,
    /// The operator requires a text literal.
    ExpectedText,
    /// The operator requires a scalar literal.
    ExpectedScalar,
    /// The literal does not match the accepted field type.
    LiteralTypeMismatch,
    /// A list element does not match the accepted element type.
    ListElementTypeMismatch,
    /// The nominal enum path differs from the accepted field type.
    EnumPathMismatch,
    /// The enum variant is absent from the accepted enum contract.
    UnknownEnumVariant,
    /// The payload does not match the accepted variant's body contract.
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
    /// The requested field is absent from the accepted schema.
    UnknownField {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("field '{field}' is not queryable")]
    /// The field's accepted type does not support query evaluation.
    NonQueryableFieldType {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("duplicate field '{field}'")]
    /// The field is declared more than once.
    DuplicateField {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("map predicates are unsupported for field '{field}'")]
    /// Map-valued fields do not support this predicate surface.
    MapPredicateUnsupported {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("primary key '{field}' not present in entity fields")]
    /// The primary key is absent from the entity's fields.
    InvalidPrimaryKey {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("primary key '{field}' has a non-keyable type")]
    /// The primary-key field type cannot be encoded as a key.
    InvalidPrimaryKeyType {
        /// Name of the field being evaluated or rejected.
        field: String,
    },

    #[error("duplicate index name '{name}'")]
    /// Two indexes share the same name.
    DuplicateIndexName {
        /// Conflicting index name.
        name: String,
    },

    #[error("operator {operator} is not valid for field '{field}'")]
    /// The operator is not admitted for the field type.
    InvalidOperator {
        /// Name of the field being evaluated or rejected.
        field: String,
        /// Operator rejected for this field.
        operator: SchemaValidationOperator,
    },

    #[error("coercion {coercion:?} is not valid for field '{field}'")]
    /// The coercion is not admitted for the field type.
    InvalidCoercion {
        /// Name of the field being evaluated or rejected.
        field: String,
        /// Requested coercion strategy.
        coercion: CoercionId,
    },

    #[error("invalid literal for field '{field}': {reason}")]
    /// The literal violates the accepted field contract.
    InvalidLiteral {
        /// Name of the field being evaluated or rejected.
        field: String,
        /// Typed cause of the rejection.
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
