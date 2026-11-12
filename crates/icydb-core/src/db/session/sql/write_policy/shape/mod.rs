//! Module: db::session::sql::write_policy::shape
//! Responsibility: parser-shape classification for shared SQL write policies.
//! Does not own: execution bounds, validated plan wrappers, or statement-family policy.
//! Boundary: inspects parsed SQL expressions into shared write-shape proof DTOs.

use super::model::{
    SqlWriteOrderProof, SqlWriteReturningShape, SqlWriteStatementShape, SqlWriteWhereProof,
};
use crate::db::sql::parser::{
    SqlExpr, SqlExprBinaryOp, SqlOrderDirection, SqlOrderTerm, SqlReturningProjection,
};
use std::collections::BTreeSet;

pub(in crate::db::session::sql) struct SqlWriteStatementShapeInput<'a> {
    pub(in crate::db::session::sql) predicate: Option<&'a SqlExpr>,
    pub(in crate::db::session::sql) entity: &'a str,
    pub(in crate::db::session::sql) table_alias: Option<&'a str>,
    pub(in crate::db::session::sql) order_by: &'a [SqlOrderTerm],
    pub(in crate::db::session::sql) limit: Option<u32>,
    pub(in crate::db::session::sql) offset: Option<u32>,
    pub(in crate::db::session::sql) returning: Option<&'a SqlReturningProjection>,
    pub(in crate::db::session::sql) primary_key_fields: &'a [&'a str],
}

pub(in crate::db::session::sql) fn classify_write_statement_shape(
    input: SqlWriteStatementShapeInput<'_>,
) -> SqlWriteStatementShape {
    SqlWriteStatementShape {
        where_proof: classify_write_where_proof(
            input.predicate,
            input.entity,
            input.table_alias,
            input.primary_key_fields,
        ),
        order_proof: classify_write_order_proof(
            input.order_by,
            input.entity,
            input.table_alias,
            input.primary_key_fields,
        ),
        limit: input.limit,
        offset: input.offset,
        returning_shape: classify_write_returning_shape(input.returning),
    }
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
        | SqlExpr::BooleanTest { .. }
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
        | SqlExpr::BooleanTest { .. }
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
mod tests;
