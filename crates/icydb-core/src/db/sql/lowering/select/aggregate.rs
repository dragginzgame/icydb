use crate::db::sql::lowering::{SqlLoweringError, aggregate::resolve_having_aggregate_index};
use crate::{
    db::{
        predicate::CompareOp,
        query::plan::expr::{BinaryOp, Function},
        sql::parser::{
            SqlAggregateCall, SqlArithmeticProjectionOp, SqlHavingClause, SqlHavingValueExpr,
            SqlProjection, SqlProjectionOperand, SqlRoundProjectionCall, SqlRoundProjectionInput,
        },
    },
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
    if having_clauses.is_empty() {
        return Ok(Vec::new());
    }
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::having_requires_group_by());
    }

    let SqlProjection::Items(_) = projection else {
        return Err(SqlLoweringError::unsupported_select_having());
    };

    let mut lowered = Vec::with_capacity(having_clauses.len());
    for clause in having_clauses {
        lowered.push(ResolvedHavingClause {
            expr: lower_having_expr(clause, grouped_aggregates)?,
        });
    }

    Ok(lowered)
}

fn lower_having_expr(
    clause: SqlHavingClause,
    grouped_aggregates: &[SqlAggregateCall],
) -> Result<ResolvedHavingExpr, SqlLoweringError> {
    Ok(ResolvedHavingExpr::Compare {
        left: lower_having_value_expr(clause.left, grouped_aggregates)?,
        op: clause.op,
        right: lower_having_value_expr(clause.right, grouped_aggregates)?,
    })
}

fn lower_having_value_expr(
    expr: SqlHavingValueExpr,
    grouped_aggregates: &[SqlAggregateCall],
) -> Result<ResolvedHavingValueExpr, SqlLoweringError> {
    match expr {
        SqlHavingValueExpr::Field(field) => Ok(ResolvedHavingValueExpr::GroupField(field)),
        SqlHavingValueExpr::Aggregate(aggregate) => Ok(ResolvedHavingValueExpr::AggregateIndex(
            resolve_having_aggregate_index(&aggregate, grouped_aggregates)?,
        )),
        SqlHavingValueExpr::Literal(literal) => Ok(ResolvedHavingValueExpr::Literal(literal)),
        SqlHavingValueExpr::Arithmetic(call) => Ok(ResolvedHavingValueExpr::Binary {
            op: lower_having_binary_op(call.op),
            left: Box::new(lower_having_operand_expr(call.left, grouped_aggregates)?),
            right: Box::new(lower_having_operand_expr(call.right, grouped_aggregates)?),
        }),
        SqlHavingValueExpr::Round(call) => Ok(ResolvedHavingValueExpr::FunctionCall {
            function: Function::Round,
            args: lower_having_round_args(call, grouped_aggregates)?,
        }),
    }
}

fn lower_having_round_args(
    call: SqlRoundProjectionCall,
    grouped_aggregates: &[SqlAggregateCall],
) -> Result<Vec<ResolvedHavingValueExpr>, SqlLoweringError> {
    let value_expr = match call.input {
        SqlRoundProjectionInput::Operand(operand) => {
            lower_having_operand_expr(operand, grouped_aggregates)?
        }
        SqlRoundProjectionInput::Arithmetic(call) => ResolvedHavingValueExpr::Binary {
            op: lower_having_binary_op(call.op),
            left: Box::new(lower_having_operand_expr(call.left, grouped_aggregates)?),
            right: Box::new(lower_having_operand_expr(call.right, grouped_aggregates)?),
        },
    };

    Ok(vec![
        value_expr,
        ResolvedHavingValueExpr::Literal(call.scale),
    ])
}

fn lower_having_operand_expr(
    operand: SqlProjectionOperand,
    grouped_aggregates: &[SqlAggregateCall],
) -> Result<ResolvedHavingValueExpr, SqlLoweringError> {
    match operand {
        SqlProjectionOperand::Field(field) => Ok(ResolvedHavingValueExpr::GroupField(field)),
        SqlProjectionOperand::Aggregate(aggregate) => Ok(ResolvedHavingValueExpr::AggregateIndex(
            resolve_having_aggregate_index(&aggregate, grouped_aggregates)?,
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
