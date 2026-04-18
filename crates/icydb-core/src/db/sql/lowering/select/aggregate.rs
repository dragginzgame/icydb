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
                aggregate::resolve_having_aggregate_expr_index,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::{
                SqlAggregateCall, SqlExpr, SqlHavingClause, SqlProjection, SqlProjectionOperand,
                SqlRoundProjectionInput,
            },
        },
    },
    model::entity::EntityModel,
};

/// Lower grouped SQL `HAVING` clauses onto planner-owned expressions.
///
/// This keeps grouped `HAVING` on the shared `SqlExpr -> Expr` seam and
/// canonicalizes numeric literals against grouped-key field kinds once the
/// concrete entity model is available.
pub(super) fn lower_having_clauses(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_aggregates: &[SqlAggregateCall],
    model: &'static EntityModel,
) -> Result<Vec<Expr>, SqlLoweringError> {
    lower_having_clauses_with_policy(
        having_clauses,
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
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
) -> Result<Option<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let mut clauses = lower_having_clauses_with_policy(
        having_clauses,
        projection,
        |aggregate| resolve_aggregate_index(aggregate),
        false,
    )?;
    if clauses.is_empty() {
        return Ok(None);
    }

    for expr in &clauses {
        register_global_having_aggregates(expr, &mut resolve_aggregate_index)?;
    }

    Ok(Some(combine_having_clauses(clauses.split_off(0))))
}

fn lower_having_clauses_with_policy<F>(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
    require_group_by: bool,
) -> Result<Vec<Expr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    if having_clauses.is_empty() {
        return Ok(Vec::new());
    }
    if require_group_by {
        return Err(SqlLoweringError::having_requires_group_by());
    }

    let SqlProjection::Items(_) = projection else {
        return Err(SqlLoweringError::unsupported_select_having());
    };

    let mut lowered = Vec::with_capacity(having_clauses.len());
    for clause in having_clauses {
        lowered.push(lower_having_expr_with_policy(
            clause,
            &mut resolve_aggregate_index,
        )?);
    }

    Ok(lowered)
}

fn lower_having_expr_with_policy<F>(
    clause: SqlHavingClause,
    resolve_aggregate_index: &mut F,
) -> Result<Expr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let left = lower_having_value_expr_with_policy(clause.left, resolve_aggregate_index)?;
    let right = lower_having_value_expr_with_policy(clause.right, resolve_aggregate_index)?;

    Ok(Expr::Binary {
        op: compare_op_to_binary_op(clause.op),
        left: Box::new(left),
        right: Box::new(right),
    })
}

fn lower_having_value_expr_with_policy<F>(
    expr: SqlExpr,
    resolve_aggregate_index: &mut F,
) -> Result<Expr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let expr = lower_sql_expr(&expr, SqlExprPhase::PostAggregate)?;
    register_having_aggregates(&expr, resolve_aggregate_index)?;

    Ok(expr)
}

fn register_having_aggregates<F>(
    expr: &Expr,
    resolve_aggregate_index: &mut F,
) -> Result<(), SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    match expr {
        Expr::Field(_) | Expr::Literal(_) => Ok(()),
        Expr::Aggregate(aggregate) => resolve_aggregate_index(aggregate).map(|_| ()),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                register_having_aggregates(arg, resolve_aggregate_index)?;
            }

            Ok(())
        }
        Expr::Unary { expr, .. } => register_having_aggregates(expr, resolve_aggregate_index),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                register_having_aggregates(arm.condition(), resolve_aggregate_index)?;
                register_having_aggregates(arm.result(), resolve_aggregate_index)?;
            }

            register_having_aggregates(else_expr, resolve_aggregate_index)
        }
        Expr::Binary { left, right, .. } => {
            register_having_aggregates(left, resolve_aggregate_index)?;
            register_having_aggregates(right, resolve_aggregate_index)
        }
        #[cfg(test)]
        Expr::Alias { expr, name: _ } => register_having_aggregates(expr, resolve_aggregate_index),
    }
}

fn register_global_having_aggregates<F>(
    expr: &Expr,
    resolve_aggregate_index: &mut F,
) -> Result<(), SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    match expr {
        Expr::Field(_) => Err(SqlLoweringError::unsupported_select_having()),
        Expr::Literal(_) => Ok(()),
        Expr::Aggregate(aggregate) => resolve_aggregate_index(aggregate).map(|_| ()),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                register_global_having_aggregates(arg, resolve_aggregate_index)?;
            }

            Ok(())
        }
        Expr::Unary { expr, .. } => {
            register_global_having_aggregates(expr, resolve_aggregate_index)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                register_global_having_aggregates(arm.condition(), resolve_aggregate_index)?;
                register_global_having_aggregates(arm.result(), resolve_aggregate_index)?;
            }

            register_global_having_aggregates(else_expr, resolve_aggregate_index)
        }
        Expr::Binary { left, right, .. } => {
            register_global_having_aggregates(left, resolve_aggregate_index)?;
            register_global_having_aggregates(right, resolve_aggregate_index)
        }
        #[cfg(test)]
        Expr::Alias { expr, name: _ } => {
            register_global_having_aggregates(expr, resolve_aggregate_index)
        }
    }
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
    having_clauses: &[SqlHavingClause],
) {
    for clause in having_clauses {
        collect_sql_expr_aggregate_calls(&clause.left, aggregate_calls);
        collect_sql_expr_aggregate_calls(&clause.right, aggregate_calls);
    }
}

fn collect_sql_expr_aggregate_calls(expr: &SqlExpr, aggregate_calls: &mut Vec<SqlAggregateCall>) {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Literal(_) | SqlExpr::TextFunction(_) => {}
        SqlExpr::Aggregate(aggregate) => {
            push_unique_grouped_having_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlExpr::NullTest { expr, .. } | SqlExpr::Unary { expr, .. } => {
            collect_sql_expr_aggregate_calls(expr, aggregate_calls);
        }
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                collect_sql_expr_aggregate_calls(arg, aggregate_calls);
            }
        }
        SqlExpr::Round(call) => collect_round_input_aggregate_calls(&call.input, aggregate_calls),
        SqlExpr::Binary { left, right, .. } => {
            collect_sql_expr_aggregate_calls(left, aggregate_calls);
            collect_sql_expr_aggregate_calls(right, aggregate_calls);
        }
        SqlExpr::Case { arms, else_expr } => {
            for arm in arms {
                collect_sql_expr_aggregate_calls(&arm.condition, aggregate_calls);
                collect_sql_expr_aggregate_calls(&arm.result, aggregate_calls);
            }

            if let Some(else_expr) = else_expr {
                collect_sql_expr_aggregate_calls(else_expr, aggregate_calls);
            }
        }
    }
}

fn collect_round_input_aggregate_calls(
    input: &SqlRoundProjectionInput,
    aggregate_calls: &mut Vec<SqlAggregateCall>,
) {
    match input {
        SqlRoundProjectionInput::Operand(operand) => {
            collect_projection_operand_aggregate_calls(operand, aggregate_calls);
        }
        SqlRoundProjectionInput::Arithmetic(call) => {
            collect_projection_operand_aggregate_calls(&call.left, aggregate_calls);
            collect_projection_operand_aggregate_calls(&call.right, aggregate_calls);
        }
    }
}

fn collect_projection_operand_aggregate_calls(
    operand: &SqlProjectionOperand,
    aggregate_calls: &mut Vec<SqlAggregateCall>,
) {
    match operand {
        SqlProjectionOperand::Field(_) | SqlProjectionOperand::Literal(_) => {}
        SqlProjectionOperand::Aggregate(aggregate) => {
            push_unique_grouped_having_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlProjectionOperand::Arithmetic(call) => {
            collect_projection_operand_aggregate_calls(&call.left, aggregate_calls);
            collect_projection_operand_aggregate_calls(&call.right, aggregate_calls);
        }
    }
}

fn push_unique_grouped_having_aggregate_call(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    aggregate: SqlAggregateCall,
) {
    if aggregate_calls.iter().all(|current| current != &aggregate) {
        aggregate_calls.push(aggregate);
    }
}

const fn compare_op_to_binary_op(op: crate::db::predicate::CompareOp) -> BinaryOp {
    match op {
        crate::db::predicate::CompareOp::Ne => BinaryOp::Ne,
        crate::db::predicate::CompareOp::Lt => BinaryOp::Lt,
        crate::db::predicate::CompareOp::Lte => BinaryOp::Lte,
        crate::db::predicate::CompareOp::Gt => BinaryOp::Gt,
        crate::db::predicate::CompareOp::Gte => BinaryOp::Gte,
        crate::db::predicate::CompareOp::Eq
        | crate::db::predicate::CompareOp::Contains
        | crate::db::predicate::CompareOp::StartsWith
        | crate::db::predicate::CompareOp::EndsWith
        | crate::db::predicate::CompareOp::In
        | crate::db::predicate::CompareOp::NotIn => BinaryOp::Eq,
    }
}
