//! Module: sql::lowering::predicate
//! Responsibility: lower SQL boolean expressions into predicate and planner expression forms.
//! Does not own: predicate normalization semantics or executor route selection.
//! Boundary: translates parser SQL shapes onto runtime predicate/query-plan authorities.

mod normalize;
#[cfg(test)]
mod tests;
mod validate;

use crate::{
    db::{
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate,
            collapse_membership_values, normalize_owned,
        },
        query::plan::expr::{
            Expr, derive_normalized_bool_expr_predicate_subset, is_normalized_bool_expr,
        },
        sql::{
            lowering::{
                SqlLoweringError,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{SqlExpr, SqlExprBinaryOp, SqlScalarFunction},
        },
    },
    value::Value,
};

// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
// authority through the shared SQL-expression seam.
pub(in crate::db::sql::lowering) fn lower_sql_where_expr(
    expr: &SqlExpr,
) -> Result<Predicate, SqlLoweringError> {
    let lowered_expr = lower_sql_where_bool_expr(expr)?;

    derive_sql_where_expr_predicate_subset(expr, &lowered_expr)
        .ok_or_else(SqlLoweringError::unsupported_where_expression)
}

// Derive the predicate subset for one already-lowered SQL WHERE expression.
// Top-level membership can stay compact instead of expanding through an
// OR/AND expression chain and collapsing back to `IN`/`NOT IN` later.
pub(in crate::db::sql::lowering) fn derive_sql_where_expr_predicate_subset(
    sql_expr: &SqlExpr,
    lowered_expr: &Expr,
) -> Option<Predicate> {
    derive_top_level_sql_membership_predicate_subset(sql_expr)
        .or_else(|| derive_normalized_bool_expr_predicate_subset(lowered_expr))
}

// Derive a fully-owned predicate for one parser-level SQL WHERE shape without
// first lowering a visible expression. This is intentionally narrow: callers
// may skip the visible filter only when parser context proves the predicate is
// the complete semantic filter.
#[cfg(test)]
pub(in crate::db::sql::lowering) fn derive_sql_where_expr_predicate_only_subset(
    sql_expr: &SqlExpr,
) -> Option<Predicate> {
    if !predicate_only_sql_expr_contains_membership(sql_expr) {
        return None;
    }

    derive_sql_where_expr_predicate_only_subset_impl(sql_expr).map(normalize_owned)
}

pub(in crate::db::sql::lowering) fn derive_sql_where_expr_predicate_only_subset_owned(
    sql_expr: SqlExpr,
) -> Result<Predicate, SqlExpr> {
    if !predicate_only_sql_expr_contains_membership(&sql_expr) {
        return Err(sql_expr);
    }

    let Some(plan) = predicate_only_sql_expr_plan(&sql_expr) else {
        return Err(sql_expr);
    };
    let predicate = derive_sql_where_expr_predicate_only_subset_owned_impl(sql_expr, &plan);

    Ok(normalize_owned(predicate))
}

enum PredicateOnlySqlExprPlan {
    Membership { coercion: CoercionId },
    And(Box<Self>, Box<Self>),
    Compare,
}

#[cfg(test)]
fn derive_sql_where_expr_predicate_only_subset_impl(sql_expr: &SqlExpr) -> Option<Predicate> {
    match sql_expr {
        SqlExpr::Membership { .. } => derive_top_level_sql_membership_predicate_subset(sql_expr),
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => Some(Predicate::And(vec![
            derive_sql_where_expr_predicate_only_subset_impl(left.as_ref())?,
            derive_sql_where_expr_predicate_only_subset_impl(right.as_ref())?,
        ])),
        SqlExpr::Binary { op, left, right } => {
            derive_sql_binary_compare_predicate(*op, left.as_ref(), right.as_ref())
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Case { .. } => None,
    }
}

fn derive_sql_where_expr_predicate_only_subset_owned_impl(
    sql_expr: SqlExpr,
    plan: &PredicateOnlySqlExprPlan,
) -> Predicate {
    match (sql_expr, plan) {
        (
            SqlExpr::Membership {
                expr,
                values,
                negated,
            },
            PredicateOnlySqlExprPlan::Membership { coercion },
        ) => derive_top_level_sql_membership_predicate_subset_owned(
            *expr, values, negated, *coercion,
        ),
        (
            SqlExpr::Binary {
                op: SqlExprBinaryOp::And,
                left,
                right,
            },
            PredicateOnlySqlExprPlan::And(left_plan, right_plan),
        ) => Predicate::And(vec![
            derive_sql_where_expr_predicate_only_subset_owned_impl(*left, left_plan),
            derive_sql_where_expr_predicate_only_subset_owned_impl(*right, right_plan),
        ]),
        (SqlExpr::Binary { op, left, right }, PredicateOnlySqlExprPlan::Compare) => {
            derive_sql_binary_compare_predicate_owned(op, *left, *right)
                .expect("predicate-only plan must match owned compare lowering")
        }
        _ => unreachable!("predicate-only plan must match admitted SQL expression"),
    }
}

fn predicate_only_sql_expr_plan(sql_expr: &SqlExpr) -> Option<PredicateOnlySqlExprPlan> {
    match sql_expr {
        SqlExpr::Membership { expr, values, .. } => {
            let (_, fixed_coercion) = sql_membership_target(expr.as_ref())?;

            Some(PredicateOnlySqlExprPlan::Membership {
                coercion: sql_membership_values_coercion(values, fixed_coercion)?,
            })
            .filter(|_| values.len() >= 2)
        }
        SqlExpr::Binary {
            op: SqlExprBinaryOp::And,
            left,
            right,
        } => Some(PredicateOnlySqlExprPlan::And(
            Box::new(predicate_only_sql_expr_plan(left.as_ref())?),
            Box::new(predicate_only_sql_expr_plan(right.as_ref())?),
        )),
        SqlExpr::Binary { op, left, right } => {
            if sql_binary_compare_predicate_supported(*op, left.as_ref(), right.as_ref()) {
                Some(PredicateOnlySqlExprPlan::Compare)
            } else {
                None
            }
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Case { .. } => None,
    }
}

fn predicate_only_sql_expr_contains_membership(sql_expr: &SqlExpr) -> bool {
    match sql_expr {
        SqlExpr::Membership { .. } => true,
        SqlExpr::Binary { op: _, left, right } => {
            predicate_only_sql_expr_contains_membership(left.as_ref())
                || predicate_only_sql_expr_contains_membership(right.as_ref())
        }
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Unary { .. }
        | SqlExpr::Case { .. } => false,
    }
}

#[cfg(test)]
fn derive_sql_binary_compare_predicate(
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
) -> Option<Predicate> {
    let op = sql_compare_op(op)?;
    if matches!(left, SqlExpr::Literal(Value::Null))
        || matches!(right, SqlExpr::Literal(Value::Null))
    {
        return None;
    }

    match (left, right) {
        (SqlExpr::Field(field), SqlExpr::Literal(value)) => {
            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field,
                op,
                value.clone(),
                sql_compare_literal_coercion(op, value),
            )))
        }
        (SqlExpr::Literal(value), SqlExpr::Field(field)) => {
            let op = op.flipped();

            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field,
                op,
                value.clone(),
                sql_compare_literal_coercion(op, value),
            )))
        }
        (SqlExpr::Field(left_field), SqlExpr::Field(right_field)) => Some(
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                left_field,
                op,
                right_field,
                sql_compare_field_coercion(op),
            )),
        ),
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args,
            },
            SqlExpr::Literal(Value::Text(value)),
        ) => {
            let [SqlExpr::Field(field)] = args.as_slice() else {
                return None;
            };

            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field,
                op,
                Value::Text(value.clone()),
                CoercionId::TextCasefold,
            )))
        }
        _ => None,
    }
}

fn derive_sql_binary_compare_predicate_owned(
    op: SqlExprBinaryOp,
    left: SqlExpr,
    right: SqlExpr,
) -> Option<Predicate> {
    let op = sql_compare_op(op)?;
    if matches!(left, SqlExpr::Literal(Value::Null))
        || matches!(right, SqlExpr::Literal(Value::Null))
    {
        return None;
    }

    match (left, right) {
        (SqlExpr::Field(field), SqlExpr::Literal(value)) => {
            let coercion = sql_compare_literal_coercion(op, &value);

            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field, op, value, coercion,
            )))
        }
        (SqlExpr::Literal(value), SqlExpr::Field(field)) => {
            let op = op.flipped();
            let coercion = sql_compare_literal_coercion(op, &value);

            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field, op, value, coercion,
            )))
        }
        (SqlExpr::Field(left_field), SqlExpr::Field(right_field)) => Some(
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                left_field,
                op,
                right_field,
                sql_compare_field_coercion(op),
            )),
        ),
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args,
            },
            SqlExpr::Literal(Value::Text(value)),
        ) => {
            if args.len() != 1 {
                return None;
            }
            let Some(SqlExpr::Field(field)) = args.into_iter().next() else {
                return None;
            };

            Some(Predicate::Compare(ComparePredicate::with_coercion(
                field,
                op,
                Value::Text(value),
                CoercionId::TextCasefold,
            )))
        }
        _ => None,
    }
}

fn sql_binary_compare_predicate_supported(
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
) -> bool {
    if sql_compare_op(op).is_none() {
        return false;
    }
    if matches!(left, SqlExpr::Literal(Value::Null))
        || matches!(right, SqlExpr::Literal(Value::Null))
    {
        return false;
    }

    match (left, right) {
        (SqlExpr::Field(_), SqlExpr::Literal(_) | SqlExpr::Field(_))
        | (SqlExpr::Literal(_), SqlExpr::Field(_)) => true,
        (
            SqlExpr::FunctionCall {
                function: SqlScalarFunction::Lower,
                args,
            },
            SqlExpr::Literal(Value::Text(_)),
        ) => matches!(args.as_slice(), [SqlExpr::Field(_)]),
        _ => false,
    }
}

const fn sql_compare_op(op: SqlExprBinaryOp) -> Option<CompareOp> {
    match op {
        SqlExprBinaryOp::Eq => Some(CompareOp::Eq),
        SqlExprBinaryOp::Ne => Some(CompareOp::Ne),
        SqlExprBinaryOp::Lt => Some(CompareOp::Lt),
        SqlExprBinaryOp::Lte => Some(CompareOp::Lte),
        SqlExprBinaryOp::Gt => Some(CompareOp::Gt),
        SqlExprBinaryOp::Gte => Some(CompareOp::Gte),
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Add
        | SqlExprBinaryOp::Sub
        | SqlExprBinaryOp::Mul
        | SqlExprBinaryOp::Div => None,
    }
}

const fn sql_compare_literal_coercion(op: CompareOp, value: &Value) -> CoercionId {
    match value {
        Value::Text(_) | Value::Nat64(_) | Value::Nat128(_) | Value::NatBig(_) => {
            CoercionId::Strict
        }
        Value::Float32(_) | Value::Float64(_) | Value::Decimal(_) => {
            if op.is_ordering_family() {
                CoercionId::NumericWiden
            } else {
                CoercionId::Strict
            }
        }
        _ if value.supports_numeric_coercion() => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    }
}

fn sql_compare_field_coercion(op: CompareOp) -> CoercionId {
    if !op.supports_field_compare() {
        unreachable!("sql predicate lowering invariant");
    }

    if op.is_ordering_family() {
        CoercionId::NumericWiden
    } else {
        CoercionId::Strict
    }
}

// Lower one parser-owned SQL boolean expression onto the shared planner-owned
// WHERE boolean seam without compiling it into the runtime predicate layer.
pub(in crate::db::sql::lowering) fn lower_sql_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, false, SqlExprPhase::Where)
}

// Lower one SQL boolean expression that uses WHERE admission rules but does
// not own the top-level WHERE-only text-predicate casefold compatibility path.
pub(in crate::db::sql::lowering) fn lower_sql_pre_aggregate_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, false, SqlExprPhase::PreAggregate)
}

// Lower one parser-owned SQL scalar-row boolean expression through the
// bounded scalar searched-`CASE` canonicalization seam without changing the
// grouped or aggregate filter-expression surfaces.
pub(in crate::db::sql::lowering) fn lower_sql_scalar_where_bool_expr(
    expr: &SqlExpr,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_bool_expr_internal(expr, true, SqlExprPhase::Where)
}

fn lower_sql_bool_expr_internal(
    expr: &SqlExpr,
    scalar_case_canonicalization: bool,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    let expr = lower_sql_expr(expr, phase)?;
    validate::validate_where_bool_expr(&expr)?;
    let expr = if scalar_case_canonicalization {
        normalize::normalize_scalar_where_bool_expr(expr)
    } else {
        normalize::normalize_where_bool_expr(expr)
    };

    debug_assert!(
        validate::validate_where_bool_expr(&expr).is_ok(),
        "WHERE normalization must not widen or narrow clause admissibility",
    );

    debug_assert!(is_normalized_bool_expr(&expr));

    Ok(expr)
}

fn derive_top_level_sql_membership_predicate_subset(expr: &SqlExpr) -> Option<Predicate> {
    let SqlExpr::Membership {
        expr,
        values,
        negated,
    } = expr
    else {
        return None;
    };

    let target_op = if *negated {
        CompareOp::NotIn
    } else {
        CompareOp::In
    };
    let (field, fixed_coercion) = sql_membership_target(expr.as_ref())?;

    let mut admitted_values = Vec::with_capacity(values.len());
    let mut admitted_coercion = None;
    for value in values {
        let coercion = sql_membership_value_coercion(value, fixed_coercion)?;
        if let Some(current) = admitted_coercion {
            if current != coercion {
                return None;
            }
        } else {
            admitted_coercion = Some(coercion);
        }
        admitted_values.push(value.clone());
    }

    collapse_membership_values(field, target_op, admitted_values, admitted_coercion?)
        .map(Predicate::Compare)
}

fn derive_top_level_sql_membership_predicate_subset_owned(
    expr: SqlExpr,
    values: Vec<Value>,
    negated: bool,
    coercion: CoercionId,
) -> Predicate {
    let target_op = if negated {
        CompareOp::NotIn
    } else {
        CompareOp::In
    };
    let (field, _) = sql_membership_target_owned(expr)
        .expect("predicate-only plan must admit membership target");

    collapse_membership_values(&field, target_op, values, coercion)
        .map(Predicate::Compare)
        .expect("predicate-only plan must admit membership values")
}

fn sql_membership_target(expr: &SqlExpr) -> Option<(&str, Option<CoercionId>)> {
    match expr {
        SqlExpr::Field(field) => Some((field.as_str(), None)),
        SqlExpr::FunctionCall {
            function: SqlScalarFunction::Lower,
            args,
        } => match args.as_slice() {
            [SqlExpr::Field(field)] => Some((field.as_str(), Some(CoercionId::TextCasefold))),
            _ => None,
        },
        _ => None,
    }
}

fn sql_membership_target_owned(expr: SqlExpr) -> Option<(String, Option<CoercionId>)> {
    match expr {
        SqlExpr::Field(field) => Some((field, None)),
        SqlExpr::FunctionCall {
            function: SqlScalarFunction::Lower,
            args,
        } => {
            if args.len() != 1 {
                return None;
            }
            let Some(SqlExpr::Field(field)) = args.into_iter().next() else {
                return None;
            };

            Some((field, Some(CoercionId::TextCasefold)))
        }
        _ => None,
    }
}

fn sql_membership_values_coercion(
    values: &[Value],
    fixed: Option<CoercionId>,
) -> Option<CoercionId> {
    let mut admitted_coercion = None;
    for value in values {
        let coercion = sql_membership_value_coercion(value, fixed)?;
        if let Some(current) = admitted_coercion {
            if current != coercion {
                return None;
            }
        } else {
            admitted_coercion = Some(coercion);
        }
    }

    admitted_coercion
}

const fn sql_membership_value_coercion(
    value: &Value,
    fixed: Option<CoercionId>,
) -> Option<CoercionId> {
    match fixed {
        Some(CoercionId::TextCasefold) if matches!(value, Value::Text(_)) => {
            Some(CoercionId::TextCasefold)
        }
        Some(_) => None,
        None if matches!(value, Value::List(_) | Value::Map(_)) => None,
        None if value.supports_numeric_coercion() => Some(CoercionId::NumericWiden),
        None => Some(CoercionId::Strict),
    }
}
