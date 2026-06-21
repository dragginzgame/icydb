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
