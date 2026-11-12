//! Module: query_field
//! Responsibility: compact public query-field roles and their bounded schema.
//! Does not own: rejected-field discovery, planner propagation, or rendering.
//! Boundary: validates only the public error-code/role/field contract.

use crate::ErrorCode;

/// Maximum UTF-8 byte length of one public rejected query-field reference.
pub const MAX_PUBLIC_QUERY_FIELD_BYTES: usize = 256;

/// Compact semantic role of one rejected query-field reference.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryFieldRole {
    Predicate,
    Projection,
    GroupBy,
    Having,
    OrderBy,
    AggregateTarget,
}

impl QueryFieldRole {
    /// Return the stable public wire identity.
    #[must_use]
    pub const fn raw(self) -> u8 {
        match self {
            Self::Predicate => 1,
            Self::Projection => 2,
            Self::GroupBy => 3,
            Self::Having => 4,
            Self::OrderBy => 5,
            Self::AggregateTarget => 6,
        }
    }

    /// Recover one known role from its public wire identity.
    #[must_use]
    pub const fn known(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::Predicate),
            2 => Some(Self::Projection),
            3 => Some(Self::GroupBy),
            4 => Some(Self::Having),
            5 => Some(Self::OrderBy),
            6 => Some(Self::AggregateTarget),
            _ => None,
        }
    }
}

/// Reason one optional public query-field context failed schema validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFieldSchemaMismatch {
    UnknownRole,
    DisallowedCodeRole,
    EmptyField,
    FieldTooLong,
}

/// Validate one raw public query-field context without inferring its producer.
pub fn validate_query_field_schema(
    code: ErrorCode,
    raw_role: u8,
    field: &str,
) -> Result<QueryFieldRole, QueryFieldSchemaMismatch> {
    let Some(role) = QueryFieldRole::known(raw_role) else {
        return Err(QueryFieldSchemaMismatch::UnknownRole);
    };
    if code != ErrorCode::QUERY_PLAN {
        return Err(QueryFieldSchemaMismatch::DisallowedCodeRole);
    }
    if field.is_empty() {
        return Err(QueryFieldSchemaMismatch::EmptyField);
    }
    if field.len() > MAX_PUBLIC_QUERY_FIELD_BYTES {
        return Err(QueryFieldSchemaMismatch::FieldTooLong);
    }

    Ok(role)
}

#[cfg(test)]
mod tests;
