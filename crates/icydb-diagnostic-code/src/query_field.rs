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
mod tests {
    use super::*;

    #[test]
    fn role_registry_is_exact_and_closed() {
        let roles = [
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::Having,
            QueryFieldRole::OrderBy,
            QueryFieldRole::AggregateTarget,
        ];

        for (index, role) in roles.into_iter().enumerate() {
            let raw = u8::try_from(index + 1).expect("six roles fit u8");
            assert_eq!(role.raw(), raw);
            assert_eq!(QueryFieldRole::known(raw), Some(role));
        }
        assert_eq!(QueryFieldRole::known(0), None);
        assert_eq!(QueryFieldRole::known(7), None);
        assert_eq!(QueryFieldRole::known(u8::MAX), None);
    }

    #[test]
    fn schema_accepts_only_query_plan_and_bounded_nonempty_fields() {
        for role in [
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::Having,
            QueryFieldRole::OrderBy,
            QueryFieldRole::AggregateTarget,
        ] {
            assert_eq!(
                validate_query_field_schema(ErrorCode::QUERY_PLAN, role.raw(), "missing"),
                Ok(role)
            );
        }
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_VALIDATE, 1, "missing"),
            Err(QueryFieldSchemaMismatch::DisallowedCodeRole)
        );
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_PLAN, 0, "missing"),
            Err(QueryFieldSchemaMismatch::UnknownRole)
        );
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_PLAN, 1, ""),
            Err(QueryFieldSchemaMismatch::EmptyField)
        );
    }

    #[test]
    fn schema_uses_utf8_bytes_without_truncation() {
        let exact_ascii = "a".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES);
        let over_ascii = "a".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES + 1);
        let exact_multibyte = "é".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES / 2);
        let over_multibyte = format!("{exact_multibyte}a");

        for field in [&exact_ascii, &exact_multibyte] {
            assert_eq!(
                validate_query_field_schema(ErrorCode::QUERY_PLAN, 5, field),
                Ok(QueryFieldRole::OrderBy)
            );
        }
        for field in [&over_ascii, &over_multibyte] {
            assert_eq!(
                validate_query_field_schema(ErrorCode::QUERY_PLAN, 5, field),
                Err(QueryFieldSchemaMismatch::FieldTooLong)
            );
        }
    }
}
