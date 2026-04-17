use crate::db::sql::lowering::{SqlLoweringError, aggregate::resolve_having_aggregate_index};
use crate::{
    db::{
        QueryError,
        predicate::CompareOp,
        query::plan::{
            GroupHavingExpr, GroupHavingValueExpr,
            canonicalize_grouped_having_numeric_literal_for_field_kind,
            expr::{BinaryOp, Function},
            resolve_group_field_slot,
        },
        sql::parser::{
            SqlAggregateCall, SqlArithmeticProjectionOp, SqlHavingClause, SqlHavingValueExpr,
            SqlProjection, SqlProjectionOperand, SqlRoundProjectionCall, SqlRoundProjectionInput,
        },
    },
    model::entity::EntityModel,
    value::Value,
};

///
/// ResolvedHavingValueExpr
///
/// Entity-agnostic grouped HAVING value expression after grouped projection
/// aggregate references have been resolved to stable aggregate indexes.
/// Group-field references remain textual until typed query binding resolves
/// them to canonical field slots.
///
#[derive(Clone, Debug)]
pub(super) enum ResolvedHavingValueExpr {
    GroupField(String),
    AggregateIndex(usize),
    Literal(Value),
    FunctionCall {
        function: Function,
        args: Vec<Self>,
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
        HavingFieldPolicy::Grouped,
        |aggregate| resolve_having_aggregate_index(aggregate, grouped_aggregates),
        group_by_fields.is_empty(),
    )
}

pub(in crate::db::sql::lowering) fn lower_global_aggregate_having_expr<F>(
    having_clauses: Vec<SqlHavingClause>,
    projection: &SqlProjection,
    mut resolve_aggregate_index: F,
) -> Result<Option<GroupHavingExpr>, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
{
    let clauses = lower_having_clauses_with_policy(
        having_clauses,
        projection,
        HavingFieldPolicy::RejectDirectFields,
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
    field_policy: HavingFieldPolicy,
    mut resolve_aggregate_index: F,
    require_group_by: bool,
) -> Result<Vec<ResolvedHavingClause>, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
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
            expr: lower_having_expr_with_policy(
                clause,
                field_policy,
                &mut resolve_aggregate_index,
            )?,
        });
    }

    Ok(lowered)
}

fn lower_having_expr_with_policy<F>(
    clause: SqlHavingClause,
    field_policy: HavingFieldPolicy,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingExpr, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
{
    Ok(ResolvedHavingExpr::Compare {
        left: lower_having_value_expr_with_policy(
            clause.left,
            field_policy,
            resolve_aggregate_index,
        )?,
        op: clause.op,
        right: lower_having_value_expr_with_policy(
            clause.right,
            field_policy,
            resolve_aggregate_index,
        )?,
    })
}

fn lower_having_value_expr_with_policy<F>(
    expr: SqlHavingValueExpr,
    field_policy: HavingFieldPolicy,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingValueExpr, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
{
    match expr {
        SqlHavingValueExpr::Field(field) => match field_policy {
            HavingFieldPolicy::Grouped => Ok(ResolvedHavingValueExpr::GroupField(field)),
            HavingFieldPolicy::RejectDirectFields => {
                Err(SqlLoweringError::unsupported_select_having())
            }
        },
        SqlHavingValueExpr::Aggregate(aggregate) => Ok(ResolvedHavingValueExpr::AggregateIndex(
            resolve_aggregate_index(&aggregate)?,
        )),
        SqlHavingValueExpr::Literal(literal) => Ok(ResolvedHavingValueExpr::Literal(literal)),
        SqlHavingValueExpr::Arithmetic(call) => Ok(ResolvedHavingValueExpr::Binary {
            op: lower_having_binary_op(call.op),
            left: Box::new(lower_having_operand_expr_with_policy(
                call.left,
                field_policy,
                resolve_aggregate_index,
            )?),
            right: Box::new(lower_having_operand_expr_with_policy(
                call.right,
                field_policy,
                resolve_aggregate_index,
            )?),
        }),
        SqlHavingValueExpr::Round(call) => Ok(ResolvedHavingValueExpr::FunctionCall {
            function: Function::Round,
            args: lower_having_round_args_with_policy(call, field_policy, resolve_aggregate_index)?,
        }),
    }
}

fn lower_having_round_args_with_policy<F>(
    call: SqlRoundProjectionCall,
    field_policy: HavingFieldPolicy,
    resolve_aggregate_index: &mut F,
) -> Result<Vec<ResolvedHavingValueExpr>, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
{
    let value_expr = match call.input {
        SqlRoundProjectionInput::Operand(operand) => {
            lower_having_operand_expr_with_policy(operand, field_policy, resolve_aggregate_index)?
        }
        SqlRoundProjectionInput::Arithmetic(call) => ResolvedHavingValueExpr::Binary {
            op: lower_having_binary_op(call.op),
            left: Box::new(lower_having_operand_expr_with_policy(
                call.left,
                field_policy,
                resolve_aggregate_index,
            )?),
            right: Box::new(lower_having_operand_expr_with_policy(
                call.right,
                field_policy,
                resolve_aggregate_index,
            )?),
        },
    };

    Ok(vec![
        value_expr,
        ResolvedHavingValueExpr::Literal(call.scale),
    ])
}

fn lower_having_operand_expr_with_policy<F>(
    operand: SqlProjectionOperand,
    field_policy: HavingFieldPolicy,
    resolve_aggregate_index: &mut F,
) -> Result<ResolvedHavingValueExpr, SqlLoweringError>
where
    F: FnMut(&SqlAggregateCall) -> Result<usize, SqlLoweringError>,
{
    match operand {
        SqlProjectionOperand::Field(field) => match field_policy {
            HavingFieldPolicy::Grouped => Ok(ResolvedHavingValueExpr::GroupField(field)),
            HavingFieldPolicy::RejectDirectFields => {
                Err(SqlLoweringError::unsupported_select_having())
            }
        },
        SqlProjectionOperand::Aggregate(aggregate) => Ok(ResolvedHavingValueExpr::AggregateIndex(
            resolve_aggregate_index(&aggregate)?,
        )),
        SqlProjectionOperand::Literal(literal) => Ok(ResolvedHavingValueExpr::Literal(literal)),
    }
}

pub(super) fn extend_grouped_having_aggregate_calls(
    aggregate_calls: &mut Vec<SqlAggregateCall>,
    having_clauses: &[SqlHavingClause],
) {
    for clause in having_clauses {
        collect_having_value_expr_aggregate_calls(&clause.left, aggregate_calls);
        collect_having_value_expr_aggregate_calls(&clause.right, aggregate_calls);
    }
}

#[derive(Clone, Copy)]
enum HavingFieldPolicy {
    Grouped,
    RejectDirectFields,
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
                resolve_group_field_slot(model, &field).map_err(QueryError::from)?,
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
        ResolvedHavingValueExpr::Binary { op, left, right } => Ok(GroupHavingValueExpr::Binary {
            op,
            left: Box::new(resolve_having_value_expr(model, *left)?),
            right: Box::new(resolve_having_value_expr(model, *right)?),
        }),
    }
}

fn collect_having_value_expr_aggregate_calls(
    expr: &SqlHavingValueExpr,
    aggregate_calls: &mut Vec<SqlAggregateCall>,
) {
    match expr {
        SqlHavingValueExpr::Field(_) | SqlHavingValueExpr::Literal(_) => {}
        SqlHavingValueExpr::Aggregate(aggregate) => {
            push_unique_grouped_having_aggregate_call(aggregate_calls, aggregate.clone());
        }
        SqlHavingValueExpr::Arithmetic(call) => {
            collect_having_operand_aggregate_calls(&call.left, aggregate_calls);
            collect_having_operand_aggregate_calls(&call.right, aggregate_calls);
        }
        SqlHavingValueExpr::Round(call) => match &call.input {
            SqlRoundProjectionInput::Operand(operand) => {
                collect_having_operand_aggregate_calls(operand, aggregate_calls);
            }
            SqlRoundProjectionInput::Arithmetic(call) => {
                collect_having_operand_aggregate_calls(&call.left, aggregate_calls);
                collect_having_operand_aggregate_calls(&call.right, aggregate_calls);
            }
        },
    }
}

fn collect_having_operand_aggregate_calls(
    operand: &SqlProjectionOperand,
    aggregate_calls: &mut Vec<SqlAggregateCall>,
) {
    if let SqlProjectionOperand::Aggregate(aggregate) = operand {
        push_unique_grouped_having_aggregate_call(aggregate_calls, aggregate.clone());
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

const fn lower_having_binary_op(op: SqlArithmeticProjectionOp) -> BinaryOp {
    match op {
        SqlArithmeticProjectionOp::Add => BinaryOp::Add,
        SqlArithmeticProjectionOp::Sub => BinaryOp::Sub,
        SqlArithmeticProjectionOp::Mul => BinaryOp::Mul,
        SqlArithmeticProjectionOp::Div => BinaryOp::Div,
    }
}
