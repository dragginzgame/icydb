//! Module: db::session::sql::write_policy
//! Responsibility: shared SQL write-shape proofs used by policy classifiers.
//! Does not own: statement-family admission or mutation execution.
//! Boundary: proves primary-key `WHERE`, canonical order, and `RETURNING`
//! shapes consistently for UPDATE and DELETE policy gates.

use crate::db::sql::parser::{
    SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlReturningProjection,
};
use std::collections::BTreeSet;

pub(in crate::db::session::sql) const DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT: u32 = 100;
pub(in crate::db::session::sql) const DEFAULT_PUBLIC_WRITE_RETURNING_RESPONSE_BYTES: u32 =
    1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteWhereProof {
    Missing,
    PrimaryKeyEquality,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteOrderProof {
    Missing,
    CanonicalPrimaryKey,
    DescendingPrimaryKey,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteReturningShape {
    None,
    NarrowAll,
    NarrowFields,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlWriteBoundedPolicyRejection {
    MissingCanonicalPrimaryKeyOrder,
    DescendingOrder,
    MissingLimit,
    OffsetUnsupported,
    LimitTooHigh,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWriteReturningBounds {
    pub(in crate::db::session::sql) max_rows: Option<u32>,
    pub(in crate::db::session::sql) max_response_bytes: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) struct SqlWriteExecutionBounds {
    pub(in crate::db::session::sql) max_staged_rows: Option<u32>,
    pub(in crate::db::session::sql) returning: SqlWriteReturningBounds,
}

pub(in crate::db::session::sql) fn classify_write_where_proof(
    predicate: Option<&SqlExpr>,
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> SqlWriteWhereProof {
    let Some(predicate) = predicate else {
        return SqlWriteWhereProof::Missing;
    };

    if primary_key_equality_proof(predicate, entity, table_alias, primary_key_fields) {
        SqlWriteWhereProof::PrimaryKeyEquality
    } else {
        SqlWriteWhereProof::Other
    }
}

pub(in crate::db::session::sql) fn classify_write_order_proof(
    order_by: &[SqlOrderTerm],
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> SqlWriteOrderProof {
    if order_by.is_empty() {
        return SqlWriteOrderProof::Missing;
    }
    if order_by.len() != primary_key_fields.len() {
        return SqlWriteOrderProof::Other;
    }

    let mut all_canonical = true;
    let mut saw_descending = false;
    for (term, primary_key) in order_by.iter().zip(primary_key_fields.iter().copied()) {
        let ordered_field = simple_field_name(&term.field, entity, table_alias);
        all_canonical &= ordered_field.is_some_and(|field| field == primary_key);
        saw_descending |= matches!(term.direction, SqlOrderDirection::Desc);
    }

    if !all_canonical {
        SqlWriteOrderProof::Other
    } else if saw_descending {
        SqlWriteOrderProof::DescendingPrimaryKey
    } else {
        SqlWriteOrderProof::CanonicalPrimaryKey
    }
}

pub(in crate::db::session::sql) const fn classify_write_returning_shape(
    returning: Option<&SqlReturningProjection>,
) -> SqlWriteReturningShape {
    match returning {
        None => SqlWriteReturningShape::None,
        Some(SqlReturningProjection::All) => SqlWriteReturningShape::NarrowAll,
        Some(SqlReturningProjection::Fields(_)) => SqlWriteReturningShape::NarrowFields,
    }
}

pub(in crate::db::session::sql) fn current_table_field_name<'a>(
    field: &'a str,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    let Some((qualifier, leaf)) = field.split_once('.') else {
        return Some(field);
    };
    if leaf.contains('.') {
        return None;
    }

    let qualifier_matches =
        table_alias.is_some_and(|alias| qualifier == alias) || qualifier == entity;
    qualifier_matches.then_some(leaf)
}

pub(in crate::db::session::sql) fn contains_field(fields: &[&str], field: &str) -> bool {
    fields.contains(&field)
}

pub(in crate::db::session::sql) const fn combined_optional_row_bound(
    policy_max_rows: Option<u32>,
    configured_max_rows: Option<u32>,
) -> Option<u32> {
    match (policy_max_rows, configured_max_rows) {
        (Some(policy), Some(configured)) => Some(if policy < configured {
            policy
        } else {
            configured
        }),
        (Some(policy), None) => Some(policy),
        (None, Some(configured)) => Some(configured),
        (None, None) => None,
    }
}

pub(in crate::db::session::sql) const fn bounded_write_policy_rejection(
    offset: Option<u32>,
    limit: Option<u32>,
    max_limit: u32,
    order_proof: SqlWriteOrderProof,
) -> Option<SqlWriteBoundedPolicyRejection> {
    if offset.is_some() {
        return Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported);
    }

    let Some(limit) = limit else {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    };
    if limit == 0 {
        return Some(SqlWriteBoundedPolicyRejection::MissingLimit);
    }
    if limit > max_limit {
        return Some(SqlWriteBoundedPolicyRejection::LimitTooHigh);
    }

    match order_proof {
        SqlWriteOrderProof::CanonicalPrimaryKey => None,
        SqlWriteOrderProof::DescendingPrimaryKey => {
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder)
        }
        SqlWriteOrderProof::Missing | SqlWriteOrderProof::Other => {
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder)
        }
    }
}

pub(in crate::db::session::sql) const fn sql_write_execution_bounds(
    max_staged_rows: Option<u32>,
    returning_requested: bool,
    max_returning_rows: Option<u32>,
    max_returning_response_bytes: Option<u32>,
) -> SqlWriteExecutionBounds {
    let max_rows = if returning_requested {
        combined_optional_row_bound(max_staged_rows, max_returning_rows)
    } else {
        None
    };

    SqlWriteExecutionBounds {
        max_staged_rows,
        returning: SqlWriteReturningBounds {
            max_rows,
            max_response_bytes: max_returning_response_bytes,
        },
    }
}

fn primary_key_equality_proof(
    predicate: &SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
    primary_key_fields: &[&str],
) -> bool {
    if primary_key_fields.is_empty() {
        return false;
    }

    let mut observed = BTreeSet::new();
    for leaf in conjunctive_leaves(predicate) {
        let Some(field) = primary_key_equality_field(leaf, entity, table_alias) else {
            return false;
        };
        if !contains_field(primary_key_fields, field) || !observed.insert(field.to_string()) {
            return false;
        }
    }

    primary_key_fields
        .iter()
        .all(|primary_key| observed.contains(*primary_key))
}

fn conjunctive_leaves(expr: &SqlExpr) -> Vec<&SqlExpr> {
    match expr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => {
            let mut leaves = conjunctive_leaves(left);
            leaves.extend(conjunctive_leaves(right));
            leaves
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => vec![expr],
    }
}

fn primary_key_equality_field<'a>(
    expr: &'a SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    let SqlExpr::Binary {
        op: SqlExprBinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };

    let left_field = simple_field_name(left, entity, table_alias);
    let right_field = simple_field_name(right, entity, table_alias);
    match (left_field, right_field) {
        (Some(field), None) => comparable_constant(right).then_some(field),
        (None, Some(field)) => comparable_constant(left).then_some(field),
        (Some(_), Some(_)) | (None, None) => None,
    }
}

fn simple_field_name<'a>(
    expr: &'a SqlExpr,
    entity: &str,
    table_alias: Option<&str>,
) -> Option<&'a str> {
    match expr {
        SqlExpr::Field(field) => current_table_field_name(field.as_str(), entity, table_alias),
        SqlExpr::FieldPath { root, segments } if segments.len() == 1 => {
            let qualifier_matches =
                table_alias.is_some_and(|alias| root == alias) || root == entity;
            qualifier_matches.then_some(segments[0].as_str())
        }
        SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Binary { .. }
        | SqlExpr::Case { .. } => None,
    }
}

const fn comparable_constant(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Literal(_) | SqlExpr::Param { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn literal(value: i64) -> SqlExpr {
        SqlExpr::Literal(Value::Int64(value))
    }

    fn field(name: &str) -> SqlExpr {
        SqlExpr::Field(name.to_string())
    }

    fn aliased_field(alias: &str, name: &str) -> SqlExpr {
        SqlExpr::FieldPath {
            root: alias.to_string(),
            segments: vec![name.to_string()],
        }
    }

    fn equals(left: SqlExpr, right: SqlExpr) -> SqlExpr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn and(left: SqlExpr, right: SqlExpr) -> SqlExpr {
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn bounded_write_policy_rejection_keeps_public_priority_order() {
        assert_eq!(
            bounded_write_policy_rejection(
                Some(1),
                None,
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::OffsetUnsupported),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, None, 10, SqlWriteOrderProof::CanonicalPrimaryKey,),
            Some(SqlWriteBoundedPolicyRejection::MissingLimit),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(0),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::MissingLimit),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, Some(11), 10, SqlWriteOrderProof::Other),
            Some(SqlWriteBoundedPolicyRejection::LimitTooHigh),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(10),
                10,
                SqlWriteOrderProof::DescendingPrimaryKey,
            ),
            Some(SqlWriteBoundedPolicyRejection::DescendingOrder),
        );
        assert_eq!(
            bounded_write_policy_rejection(None, Some(10), 10, SqlWriteOrderProof::Missing),
            Some(SqlWriteBoundedPolicyRejection::MissingCanonicalPrimaryKeyOrder),
        );
        assert_eq!(
            bounded_write_policy_rejection(
                None,
                Some(10),
                10,
                SqlWriteOrderProof::CanonicalPrimaryKey,
            ),
            None,
        );
    }

    #[test]
    fn sql_write_execution_bounds_combines_staged_and_returning_limits() {
        let bounded_returning = sql_write_execution_bounds(Some(10), true, Some(4), Some(1024));
        assert_eq!(bounded_returning.max_staged_rows, Some(10));
        assert_eq!(bounded_returning.returning.max_rows, Some(4));
        assert_eq!(bounded_returning.returning.max_response_bytes, Some(1024));

        let staged_only_returning = sql_write_execution_bounds(Some(10), true, None, Some(1024));
        assert_eq!(staged_only_returning.returning.max_rows, Some(10));

        let configured_only_returning = sql_write_execution_bounds(None, true, Some(4), Some(1024));
        assert_eq!(configured_only_returning.returning.max_rows, Some(4));

        let no_returning = sql_write_execution_bounds(Some(10), false, Some(4), Some(1024));
        assert_eq!(no_returning.max_staged_rows, Some(10));
        assert_eq!(no_returning.returning.max_rows, None);
        assert_eq!(no_returning.returning.max_response_bytes, Some(1024));
    }

    #[test]
    fn classify_write_order_proof_requires_full_canonical_primary_key_order() {
        let asc_id = SqlOrderTerm {
            field: SqlExpr::Field("id".to_string()),
            direction: SqlOrderDirection::Asc,
        };
        let desc_id = SqlOrderTerm {
            field: SqlExpr::Field("id".to_string()),
            direction: SqlOrderDirection::Desc,
        };
        let asc_other = SqlOrderTerm {
            field: SqlExpr::Field("name".to_string()),
            direction: SqlOrderDirection::Asc,
        };

        assert_eq!(
            classify_write_order_proof(&[], "Token", None, &["id"]),
            SqlWriteOrderProof::Missing,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&asc_id), "Token", None, &["id"]),
            SqlWriteOrderProof::CanonicalPrimaryKey,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&desc_id), "Token", None, &["id"]),
            SqlWriteOrderProof::DescendingPrimaryKey,
        );
        assert_eq!(
            classify_write_order_proof(std::slice::from_ref(&asc_other), "Token", None, &["id"]),
            SqlWriteOrderProof::Other,
        );
        assert_eq!(
            classify_write_order_proof(&[asc_id], "Token", None, &["id", "version"]),
            SqlWriteOrderProof::Other,
        );
    }

    #[test]
    fn classify_write_where_proof_requires_complete_primary_key_literal_equality() {
        let complete = and(
            equals(field("id"), literal(1)),
            equals(field("version"), literal(2)),
        );
        assert_eq!(
            classify_write_where_proof(Some(&complete), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::PrimaryKeyEquality,
        );

        let alias_complete = and(
            equals(aliased_field("t", "id"), literal(1)),
            equals(literal(2), aliased_field("t", "version")),
        );
        assert_eq!(
            classify_write_where_proof(
                Some(&alias_complete),
                "Token",
                Some("t"),
                &["id", "version"],
            ),
            SqlWriteWhereProof::PrimaryKeyEquality,
        );

        let partial = equals(field("id"), literal(1));
        assert_eq!(
            classify_write_where_proof(Some(&partial), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::Other,
        );

        let duplicate = and(
            equals(field("id"), literal(1)),
            equals(field("id"), literal(2)),
        );
        assert_eq!(
            classify_write_where_proof(Some(&duplicate), "Token", None, &["id", "version"]),
            SqlWriteWhereProof::Other,
        );

        let field_to_field = equals(field("id"), field("version"));
        assert_eq!(
            classify_write_where_proof(Some(&field_to_field), "Token", None, &["id"]),
            SqlWriteWhereProof::Other,
        );
        assert_eq!(
            classify_write_where_proof(None, "Token", None, &["id"]),
            SqlWriteWhereProof::Missing,
        );
    }
}
