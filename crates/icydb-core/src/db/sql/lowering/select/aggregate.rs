use crate::{
    db::{
        QueryError,
        predicate::CompareOp,
        query::{
            builder::AggregateExpr,
            plan::{
                GroupHavingCaseArm, GroupHavingExpr, GroupHavingValueExpr,
                canonicalize_grouped_having_numeric_literal_for_field_kind,
                expr::{BinaryOp, Expr, FieldId, Function, UnaryOp},
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
                SqlAggregateCall, SqlExpr, SqlHavingClause, SqlHavingValueExpr, SqlProjection,
                SqlProjectionOperand, SqlRoundProjectionInput,
            },
        },
    },
    model::entity::EntityModel,
    value::Value,
};

///
/// ResolvedHavingCaseArm
///
/// Entity-agnostic grouped HAVING searched-CASE arm after aggregate leaves
/// have been resolved onto stable grouped aggregate indexes and before
/// grouped field references are bound onto concrete group-field slots.
///

#[derive(Clone, Debug)]
pub(super) struct ResolvedHavingCaseArm {
    condition: ResolvedHavingValueExpr,
    result: ResolvedHavingValueExpr,
}

impl ResolvedHavingCaseArm {
    /// Build one resolved grouped HAVING searched-CASE arm.
    #[must_use]
    pub(super) const fn new(
        condition: ResolvedHavingValueExpr,
        result: ResolvedHavingValueExpr,
    ) -> Self {
        Self { condition, result }
    }

    /// Borrow the resolved grouped HAVING CASE condition.
    #[must_use]
    pub(super) const fn condition(&self) -> &ResolvedHavingValueExpr {
        &self.condition
    }

    /// Borrow the resolved grouped HAVING CASE result expression.
    #[must_use]
    pub(super) const fn result(&self) -> &ResolvedHavingValueExpr {
        &self.result
    }
}

///
/// ResolvedHavingValueExpr
///
/// Entity-agnostic grouped HAVING value expression after grouped projection
/// aggregate references have been resolved to stable aggregate indexes.
/// Group-field references remain planner-owned field ids until typed query
/// binding resolves them to canonical field slots.
///

#[derive(Clone, Debug)]
pub(super) enum ResolvedHavingValueExpr {
    GroupField(FieldId),
    AggregateIndex(usize),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Case {
        when_then_arms: Vec<ResolvedHavingCaseArm>,
        else_expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// ResolvedHavingExpr
///
/// Entity-agnostic grouped HAVING boolean expression after aggregate leaves
/// have been mapped onto stable grouped aggregate output indexes.
///

#[derive(Clone, Debug)]
pub(super) enum ResolvedHavingExpr {
    Compare {
        left: ResolvedHavingValueExpr,
        op: CompareOp,
        right: ResolvedHavingValueExpr,
    },
}

///
/// ResolvedHavingClause
///
/// Entity-agnostic grouped HAVING expression after SQL projection aggregate
/// references have been resolved to stable aggregate indexes.
///

#[derive(Clone, Debug)]
pub(super) struct ResolvedHavingClause {
    expr: ResolvedHavingExpr,
}

impl ResolvedHavingClause {
    /// Consume one resolved grouped HAVING clause into its canonical expression.
    #[must_use]
    pub(super) fn into_expr(self) -> ResolvedHavingExpr {
        self.expr
    }
}

pub(super) fn lower_having_clauses(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    group_by_fields: &[String],
    grouped_aggregates: &[SqlAggregateCall],
) -> Result<Vec<ResolvedHavingClause>, SqlLoweringError> {
    lower_having_clauses_with_policy(
        having_clauses,
        projection,
        |aggregate| resolve_having_aggregate_expr_index(aggregate, grouped_aggregates),
        group_by_fields.is_empty(),
    )
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_having_expr<F>(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
) -> Result<Option<GroupHavingExpr>, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    let clauses = lower_having_clauses_with_policy(
        having_clauses,
        projection,
        |aggregate| resolve_aggregate_index(aggregate),
        false,
    )?;
    if clauses.is_empty() {
        return Ok(None);
    }

    let mut resolved = clauses
        .into_iter()
        .map(|clause| resolve_having_expr(None, clause.into_expr()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(if resolved.len() == 1 {
        resolved
            .pop()
            .expect("global aggregate HAVING should keep one resolved clause")
    } else {
        GroupHavingExpr::And(resolved)
    }))
}

pub(in crate::db::sql::lowering) fn resolve_grouped_having_expr(
    model: &'static EntityModel,
    expr: ResolvedHavingExpr,
) -> Result<GroupHavingExpr, SqlLoweringError> {
    resolve_having_expr(Some(model), expr)
}

fn lower_having_clauses_with_policy<F>(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
    require_group_by: bool,
) -> Result<Vec<ResolvedHavingClause>, SqlLoweringError>
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
        lowered.push(ResolvedHavingClause {
            expr: lower_having_expr_with_policy(clause, &mut resolve_aggregate_index)?,
        });
    }

    Ok(lowered)
}

fn lower_having_expr_with_policy<F>(
    clause: SqlHavingClause,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingExpr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    Ok(ResolvedHavingExpr::Compare {
        left: lower_having_value_expr_with_policy(clause.left, resolve_aggregate_index)?,
        op: clause.op,
        right: lower_having_value_expr_with_policy(clause.right, resolve_aggregate_index)?,
    })
}

fn lower_having_value_expr_with_policy<F>(
    expr: SqlHavingValueExpr,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingValueExpr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    lower_having_value_expr_from_lowered_expr(
        lower_sql_expr(
            &SqlExpr::from_having_value_expr(&expr),
            SqlExprPhase::PostAggregate,
        )?,
        resolve_aggregate_index,
    )
}

fn lower_having_value_expr_from_lowered_expr<F>(
    expr: Expr,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingValueExpr, SqlLoweringError>
where
    F: FnMut(&AggregateExpr) -> Result<usize, SqlLoweringError>,
{
    match expr {
        Expr::Field(field) => Ok(ResolvedHavingValueExpr::GroupField(field)),
        Expr::Aggregate(aggregate) => Ok(ResolvedHavingValueExpr::AggregateIndex(
            resolve_aggregate_index(&aggregate)?,
        )),
        Expr::Literal(value) => Ok(ResolvedHavingValueExpr::Literal(value)),
        Expr::FunctionCall { function, args } => Ok(ResolvedHavingValueExpr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(|arg| lower_having_value_expr_from_lowered_expr(arg, resolve_aggregate_index))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Unary { op, expr } => Ok(ResolvedHavingValueExpr::Unary {
            op,
            expr: Box::new(lower_having_value_expr_from_lowered_expr(
                *expr,
                resolve_aggregate_index,
            )?),
        }),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Ok(ResolvedHavingValueExpr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    Ok(ResolvedHavingCaseArm::new(
                        lower_having_value_expr_from_lowered_expr(
                            arm.condition().clone(),
                            resolve_aggregate_index,
                        )?,
                        lower_having_value_expr_from_lowered_expr(
                            arm.result().clone(),
                            resolve_aggregate_index,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(lower_having_value_expr_from_lowered_expr(
                *else_expr,
                resolve_aggregate_index,
            )?),
        }),
        Expr::Binary { op, left, right } => Ok(ResolvedHavingValueExpr::Binary {
            op,
            left: Box::new(lower_having_value_expr_from_lowered_expr(
                *left,
                resolve_aggregate_index,
            )?),
            right: Box::new(lower_having_value_expr_from_lowered_expr(
                *right,
                resolve_aggregate_index,
            )?),
        }),
        #[cfg(test)]
        Expr::Alias { expr, name: _ } => {
            lower_having_value_expr_from_lowered_expr(*expr, resolve_aggregate_index)
        }
    }
}

pub(super) fn extend_grouped_having_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    having_clauses: &[SqlHavingClause],
) {
    for clause in having_clauses {
        collect_sql_expr_aggregate_calls(
            &SqlExpr::from_having_value_expr(&clause.left),
            aggregate_calls,
        );
        collect_sql_expr_aggregate_calls(
            &SqlExpr::from_having_value_expr(&clause.right),
            aggregate_calls,
        );
    }
}

fn resolve_having_expr(
    model: Option<&'static EntityModel>,
    expr: ResolvedHavingExpr,
) -> Result<GroupHavingExpr, SqlLoweringError> {
    match expr {
        ResolvedHavingExpr::Compare { left, op, right } => {
            let left = resolve_having_value_expr(model, left)?;
            let right = resolve_having_value_expr(model, right)?;
            let (left, right) = canonicalize_grouped_having_compare_literals(left, right);

            Ok(GroupHavingExpr::Compare { left, op, right })
        }
    }
}

fn canonicalize_grouped_having_compare_literals(
    left: GroupHavingValueExpr,
    right: GroupHavingValueExpr,
) -> (GroupHavingValueExpr, GroupHavingValueExpr) {
    match (&left, &right) {
        (GroupHavingValueExpr::GroupField(field_slot), GroupHavingValueExpr::Literal(value)) => {
            let canonical = canonicalize_grouped_having_numeric_literal_for_field_kind(
                field_slot.kind(),
                value,
            );
            (
                left,
                canonical
                    .map(GroupHavingValueExpr::Literal)
                    .unwrap_or(right),
            )
        }
        (GroupHavingValueExpr::Literal(value), GroupHavingValueExpr::GroupField(field_slot)) => {
            let canonical = canonicalize_grouped_having_numeric_literal_for_field_kind(
                field_slot.kind(),
                value,
            );
            (
                canonical.map(GroupHavingValueExpr::Literal).unwrap_or(left),
                right,
            )
        }
        _ => (left, right),
    }
}

fn resolve_having_value_expr(
    model: Option<&'static EntityModel>,
    expr: ResolvedHavingValueExpr,
) -> Result<GroupHavingValueExpr, SqlLoweringError> {
    match expr {
        ResolvedHavingValueExpr::GroupField(field) => {
            let Some(model) = model else {
                return Err(SqlLoweringError::unsupported_select_having());
            };

            Ok(GroupHavingValueExpr::GroupField(
                resolve_group_field_slot(model, field.as_str()).map_err(QueryError::from)?,
            ))
        }
        ResolvedHavingValueExpr::AggregateIndex(index) => {
            Ok(GroupHavingValueExpr::AggregateIndex(index))
        }
        ResolvedHavingValueExpr::Literal(value) => Ok(GroupHavingValueExpr::Literal(value)),
        ResolvedHavingValueExpr::FunctionCall { function, args } => {
            Ok(GroupHavingValueExpr::FunctionCall {
                function,
                args: args
                    .into_iter()
                    .map(|arg| resolve_having_value_expr(model, arg))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        ResolvedHavingValueExpr::Unary { op, expr } => Ok(GroupHavingValueExpr::Unary {
            op,
            expr: Box::new(resolve_having_value_expr(model, *expr)?),
        }),
        ResolvedHavingValueExpr::Case {
            when_then_arms,
            else_expr,
        } => Ok(GroupHavingValueExpr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    Ok(GroupHavingCaseArm::new(
                        resolve_having_value_expr(model, arm.condition().clone())?,
                        resolve_having_value_expr(model, arm.result().clone())?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(resolve_having_value_expr(model, *else_expr)?),
        }),
        ResolvedHavingValueExpr::Binary { op, left, right } => Ok(GroupHavingValueExpr::Binary {
            op,
            left: Box::new(resolve_having_value_expr(model, *left)?),
            right: Box::new(resolve_having_value_expr(model, *right)?),
        }),
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
