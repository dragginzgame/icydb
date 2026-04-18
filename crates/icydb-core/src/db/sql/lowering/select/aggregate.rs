use crate::{
    db::{
        QueryError,
        query::{
            builder::AggregateExpr,
            plan::{
                canonicalize_grouped_having_numeric_literal_for_field_kind,
                expr::{BinaryOp, Expr},
                resolve_group_field_slot,
            },
        },
        sql::{
            lowering::{
                SqlLoweringError,
                aggregate::{
                    expr_references_global_direct_fields, extend_unique_sql_expr_aggregate_calls,
                    resolve_having_aggregate_expr_index, try_for_each_expr_aggregate,
                },
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{SqlAggregateCall, SqlExpr, SqlProjection},
        },
    },
    model::entity::EntityModel,
};

/// Lower grouped SQL `HAVING` expressions onto planner-owned expressions.
///
/// This keeps grouped `HAVING` on the shared `SqlExpr -> Expr` seam and
/// canonicalizes numeric literals against grouped-key field kinds once the
/// concrete entity model is available.
pub(super) fn lower_having_clauses(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_aggregates: &[SqlAggregateCall],
    model: &'static EntityModel,
) -> Result<Vec<Expr>, SqlLoweringError> {
    lower_having_clauses_with_policy(
        having_exprs,
        projection,
        |aggregate| resolve_having_aggregate_expr_index(aggregate, grouped_aggregates),
        group_by_fields.is_empty(),
    )?
    .into_iter()
    .map(|expr| canonicalize_grouped_having_expr(model, expr))
    .collect()
}

/// Lower global aggregate SQL `HAVING` clauses onto planner-owned expressions
/// while registering any aggregate terminals needed only by `HAVING`.
pub(in crate::db::sql::lowering) fn lower_global_aggregate_having_expr<F>(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
) -> Result<Option<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let mut clauses = lower_having_clauses_with_policy(
        having_exprs,
        projection,
        |aggregate| resolve_aggregate_index(aggregate),
        false,
    )?;
    if clauses.is_empty() {
        return Ok(None);
    }

    for expr in &clauses {
        register_having_expr_aggregates(expr, &mut resolve_aggregate_index, true)?;
    }

    Ok(Some(combine_having_clauses(clauses.split_off(0))))
}

fn lower_having_clauses_with_policy<F>(
    having_exprs: Vec<SqlExpr>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
    require_group_by: bool,
) -> Result<Vec<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    if having_exprs.is_empty() {
        return Ok(Vec::new());
    }
    if require_group_by {
        return Err(SqlLoweringError::having_requires_group_by());
    }

    let SqlProjection::Items(_) = projection else {
        return Err(SqlLoweringError::unsupported_select_having());
    };

    let mut lowered = Vec::with_capacity(having_exprs.len());
    for expr in having_exprs {
        lowered.push(lower_having_expr_with_policy(
            expr,
            &mut resolve_aggregate_index,
        )?);
    }

    Ok(lowered)
}

fn lower_having_expr_with_policy<F>(
    expr: SqlExpr,
    resolve_aggregate_index: &mut F,
) -> Result<Expr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let expr = lower_having_value_expr_with_policy(expr, resolve_aggregate_index)?;

    Ok(expr)
}

fn lower_having_value_expr_with_policy<F>(
    expr: SqlExpr,
    resolve_aggregate_index: &mut F,
) -> Result<Expr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let expr = lower_sql_expr(&expr, SqlExprPhase::PostAggregate)?;
    register_having_expr_aggregates(&expr, resolve_aggregate_index, false)?;

    Ok(expr)
}

fn register_having_expr_aggregates<F>(
    expr: &Expr,
    resolve_aggregate_index: &mut F,
    reject_direct_fields: bool,
) -> Result<(), SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    if reject_direct_fields && expr_references_global_direct_fields(expr) {
        return Err(SqlLoweringError::unsupported_select_having());
    }

    try_for_each_expr_aggregate(expr, &mut |aggregate| {
        resolve_aggregate_index(aggregate).map(|_| ())
    })
}

fn combine_having_clauses(mut clauses: Vec<Expr>) -> Expr {
    if clauses.len() == 1 {
        return clauses
            .pop()
            .expect("single HAVING clause should remain present");
    }

    let mut expr = clauses.remove(0);
    for clause in clauses {
        expr = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(expr),
            right: Box::new(clause),
        };
    }

    expr
}

fn canonicalize_grouped_having_expr(
    model: &'static EntityModel,
    expr: Expr,
) -> Result<Expr, SqlLoweringError> {
    match expr {
        Expr::Field(_) | Expr::Aggregate(_) | Expr::Literal(_) => Ok(expr),
        Expr::FunctionCall { function, args } => Ok(Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| canonicalize_grouped_having_expr(model, arg))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op,
            expr: Box::new(canonicalize_grouped_having_expr(model, *expr)?),
        }),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Ok(Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    Ok(crate::db::query::plan::expr::CaseWhenArm::new(
                        canonicalize_grouped_having_expr(model, arm.condition().clone())?,
                        canonicalize_grouped_having_expr(model, arm.result().clone())?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(canonicalize_grouped_having_expr(model, *else_expr)?),
        }),
        Expr::Binary { op, left, right } => {
            let left = canonicalize_grouped_having_expr(model, *left)?;
            let right = canonicalize_grouped_having_expr(model, *right)?;
            let canonical_left = canonicalize_grouped_having_compare_literals(model, &left, &right)
                .unwrap_or_else(|| left.clone());
            let canonical_right =
                canonicalize_grouped_having_compare_literals(model, &right, &left)
                    .unwrap_or_else(|| right.clone());

            Ok(Expr::Binary {
                op,
                left: Box::new(canonical_left),
                right: Box::new(canonical_right),
            })
        }
        #[cfg(test)]
        Expr::Alias { expr, name } => Ok(Expr::Alias {
            expr: Box::new(canonicalize_grouped_having_expr(model, *expr)?),
            name,
        }),
    }
}

fn canonicalize_grouped_having_compare_literals(
    model: &'static EntityModel,
    expr: &Expr,
    other: &Expr,
) -> Option<Expr> {
    let (Expr::Literal(value), Expr::Field(field)) = (expr, other) else {
        return None;
    };
    let field_slot = resolve_group_field_slot(model, field.as_str())
        .map_err(QueryError::from)
        .ok()?;
    let canonical =
        canonicalize_grouped_having_numeric_literal_for_field_kind(field_slot.kind(), value)?;

    Some(Expr::Literal(canonical))
}

pub(super) fn extend_grouped_having_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    having_exprs: &[SqlExpr],
) {
    for expr in having_exprs {
        extend_unique_sql_expr_aggregate_calls(aggregate_calls, expr);
    }
}
