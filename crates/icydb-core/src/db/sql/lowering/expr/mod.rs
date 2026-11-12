//! Module: sql::lowering::expr
//! Responsibility: SQL expression lowering into canonical planner expressions.
//! Does not own: SQL token parsing or runtime expression evaluation.
//! Boundary: enforces clause-phase admission while translating parsed expressions.

use std::cell::Cell;

use crate::db::sql::lowering::bindings::{
    admit_bound_expression, admit_bound_membership_item, operand,
};
use crate::db::sql::lowering::{SqlLoweringError, aggregate::lower_aggregate_call};
use crate::db::{query::preparation::PreparationWork, schema::SchemaInfo};
use crate::{
    db::{
        predicate::supported_like_prefix,
        query::plan::expr::{
            BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function, UnaryOp, normalize_bool_expr,
        },
        sql::parser::{
            SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlMembershipValue, SqlScalarFunction,
        },
        sql_shared::SqlParseError,
    },
    value::{InputValue, Value},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, QueryProjectionCode, SqlFeatureCode,
};

///
/// SqlExprPhase
///
/// Lowering-time SQL expression phase boundary.
/// Clause owners pass this to the shared SQL-expression lowering seam so
/// aggregate admission stays explicit instead of leaking through wrappers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::sql::lowering) enum SqlExprPhase {
    Scalar,
    Where,
    PreAggregate,
    PostAggregate,
}

// Slot visits are invocation-local; syntax remains immutable in cached templates.
struct BoundOperands<'a> {
    schema: &'a SchemaInfo,
    values: &'a [InputValue],
    visits: Cell<usize>,
}

#[derive(Clone, Copy)]
struct SqlExprContext<'a> {
    phase: SqlExprPhase,
    operands: Option<&'a BoundOperands<'a>>,
    infer_here: bool,
}

impl SqlExprContext<'_> {
    const fn child(self, infer_here: bool) -> Self {
        Self { infer_here, ..self }
    }

    fn visits(self) -> usize {
        self.operands.map_or(0, |operands| operands.visits.get())
    }

    fn parameter(
        self,
        index: usize,
        work: &PreparationWork<'_>,
    ) -> Result<Value, SqlLoweringError> {
        let Some(operands) = self.operands else {
            return Err(SqlLoweringError::unsupported_parameter_placement(Some(
                index,
            )));
        };
        operands.visits.set(operands.visits.get() + 1);
        Ok(operand(index, operands.values, work)?)
    }
}

pub(in crate::db::sql::lowering) fn lower_sql_expr(
    expr: &SqlExpr,
    phase: SqlExprPhase,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    lower_expr(
        expr,
        SqlExprContext {
            phase,
            operands: None,
            infer_here: false,
        },
        work,
    )
}

pub(in crate::db::sql::lowering) fn lower_bound_where(
    expr: &SqlExpr,
    schema: &SchemaInfo,
    bindings: &[InputValue],
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let operands = BoundOperands {
        schema,
        values: bindings,
        visits: Cell::new(0),
    };
    lower_expr(
        expr,
        SqlExprContext {
            phase: SqlExprPhase::Where,
            operands: Some(&operands),
            infer_here: true,
        },
        work,
    )
}

// Admit each bound subtree before its parent can simplify it. Slots themselves
// retain scalar ingress admission; enclosing comparisons/functions own typing.
fn lower_expr(
    expr: &SqlExpr,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let before = context.visits();
    let lowered = lower_expr_inner(expr, context, work)?;
    if context.visits() != before
        && let Some(operands) = context.operands
    {
        let infer = context.infer_here;
        match expr {
            SqlExpr::Param { .. }
            | SqlExpr::Unary { .. }
            | SqlExpr::Membership { .. }
            | SqlExpr::Binary {
                op: SqlExprBinaryOp::And | SqlExprBinaryOp::Or,
                ..
            } => {}
            SqlExpr::Binary { op, .. }
                if infer
                    || matches!(
                        op,
                        SqlExprBinaryOp::Eq
                            | SqlExprBinaryOp::Ne
                            | SqlExprBinaryOp::Lt
                            | SqlExprBinaryOp::Lte
                            | SqlExprBinaryOp::Gt
                            | SqlExprBinaryOp::Gte
                    ) =>
            {
                admit_bound_expression(&lowered, operands.schema, infer, work)?;
            }
            _ if infer => admit_bound_expression(&lowered, operands.schema, true, work)?,
            _ => {}
        }
    }
    Ok(lowered)
}

fn lower_expr_inner(
    expr: &SqlExpr,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match expr {
        SqlExpr::Field(field) => Ok(Expr::Field(FieldId::new(copy_text(field, work)?))),
        SqlExpr::FieldPath { root, segments } => {
            let root = copy_text(root, work)?;
            charge_storage::<String>(segments.len(), work)?;
            let mut copied = Vec::with_capacity(segments.len());
            for segment in segments {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                copied.push(copy_text(segment, work)?);
            }
            Ok(Expr::FieldPath(FieldPath::new(FieldId::new(root), copied)))
        }
        SqlExpr::Aggregate(aggregate) => {
            if context.operands.is_some() {
                return Err(crate::db::QueryError::sql_lowering(
                    icydb_diagnostic_code::SqlLoweringCode::ParameterPlacement,
                )
                .into());
            }
            if !phase_allows_aggregate(context.phase) {
                return Err(phase_aggregate_error(context.phase));
            }

            Ok(Expr::Aggregate(lower_aggregate_call(aggregate, work)?))
        }
        SqlExpr::Literal(literal) => Ok(Expr::Literal(work.copy_value(literal)?)),
        SqlExpr::Param { index } => Ok(Expr::Literal(context.parameter(*index, work)?)),
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => lower_sql_membership_expr(expr.as_ref(), values.as_slice(), *negated, context, work),
        SqlExpr::BooleanTest {
            expr,
            value,
            negated,
        } => lower_sql_boolean_test(expr, *value, *negated, context, work),
        SqlExpr::NullTest { expr, negated } => {
            let arg = lower_expr(expr, context.child(false), work)?;
            charge_storage::<Expr>(1, work)?;
            Ok(Expr::FunctionCall {
                function: if *negated {
                    Function::IsNotNull
                } else {
                    Function::IsNull
                },
                args: vec![arg],
            })
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            casefold,
        } => lower_sql_like_expr(
            expr.as_ref(),
            pattern.as_str(),
            *negated,
            *casefold,
            context,
            work,
        ),
        SqlExpr::FunctionCall { function, args } => {
            lower_sql_function_call(*function, args, context, work)
        }
        SqlExpr::Unary { op, expr } => {
            let expr = lower_expr(expr, context, work)?;
            charge_storage::<Expr>(1, work)?;
            Ok(Expr::Unary {
                op: lower_sql_unary_op(*op),
                expr: Box::new(expr),
            })
        }
        SqlExpr::Binary { op, left, right } => {
            lower_sql_binary_expr(*op, left.as_ref(), right.as_ref(), context, work)
        }
        SqlExpr::Case { arms, else_expr } => {
            // Reserve the known length once; fallible collection must not
            // introduce an uncharged growth policy or a partially returned tree.
            charge_storage::<CaseWhenArm>(arms.len(), work)?;
            let mut lowered = Vec::with_capacity(arms.len());
            for arm in arms {
                lowered.push(CaseWhenArm::new(
                    lower_expr(&arm.condition, context.child(false), work)?,
                    lower_expr(&arm.result, context.child(false), work)?,
                ));
            }
            let otherwise = match else_expr.as_ref() {
                Some(else_expr) => lower_expr(else_expr.as_ref(), context.child(false), work)?,
                None => Expr::Literal(Value::Null),
            };
            charge_storage::<Expr>(1, work)?;
            Ok(Expr::Case {
                when_then_arms: lowered,
                else_expr: Box::new(otherwise),
            })
        }
    }
}

// Lower a total Boolean test without duplicating its operand or adding IR kinds.
fn lower_sql_boolean_test(
    expr: &SqlExpr,
    value: bool,
    negated: bool,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    // Default UNKNOWN to the opposite Boolean before optional inversion:
    // IS TRUE uses COALESCE(x, FALSE); IS FALSE uses NOT COALESCE(x, TRUE).
    // The same operators handle both negated forms, retaining the operand once.
    let operand = lower_expr(expr, context.child(false), work)?;
    let invert = value == negated;
    charge_storage::<Expr>(2 + usize::from(invert), work)?;
    let total = Expr::FunctionCall {
        function: Function::Coalesce,
        args: vec![operand, Expr::Literal(Value::Bool(!value))],
    };
    Ok(if invert {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(total),
        }
    } else {
        total
    })
}

// Charge requested backing immediately before allocation, not retained source
// capacity. Literal backing is charged by the shared value-copy owner.
pub(in crate::db::sql::lowering) fn charge_storage<T>(
    count: usize,
    work: &PreparationWork<'_>,
) -> Result<(), SqlLoweringError> {
    work.charge(
        Resource::TemporaryBytes,
        (count as u64).saturating_mul(size_of::<T>() as u64),
    )?;
    Ok(())
}

pub(in crate::db::sql::lowering) fn copy_text(
    text: &str,
    work: &PreparationWork<'_>,
) -> Result<String, SqlLoweringError> {
    work.charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
    charge_storage::<u8>(text.len(), work)?;
    Ok(text.to_string())
}

// Keep SQL and typed membership on the same compact planner representation.
fn lower_sql_membership_expr(
    expr: &SqlExpr,
    values: &[SqlMembershipValue],
    negated: bool,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let before = context.visits();
    let target = lower_expr(expr, context, work)?;
    let target_bound = context.visits() != before;
    charge_storage::<Value>(values.len(), work)?;
    let mut copied = Vec::with_capacity(values.len());
    for value in values {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let resolved = match value {
            SqlMembershipValue::Literal(value) => work.copy_value(value)?,
            SqlMembershipValue::Param { index } => context.parameter(*index, work)?,
        };
        if (target_bound || matches!(value, SqlMembershipValue::Param { .. }))
            && let Some(operands) = context.operands
        {
            admit_bound_membership_item(&target, &resolved, operands.schema, work)?;
        }
        copied.push(resolved);
    }
    charge_storage::<Expr>(2 + usize::from(negated), work)?;
    Ok(Expr::membership(target, copied, negated))
}

fn lower_sql_like_expr(
    expr: &SqlExpr,
    pattern: &str,
    negated: bool,
    casefold: bool,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    work.charge(Resource::PredicateExpressionSteps, pattern.len() as u64)?;
    let Some(prefix) = supported_like_prefix(pattern) else {
        return Err(crate::db::sql_shared::SqlParseError::unsupported_feature(
            SqlFeatureCode::LikePatternBeyondTrailingPrefix,
        )
        .into());
    };

    let target = lower_sql_like_target_expr(expr, casefold, context, work)?;
    let prefix = copy_text(prefix, work)?;
    charge_storage::<Expr>(2, work)?;
    let expr = Expr::FunctionCall {
        function: Function::StartsWith,
        args: vec![target, Expr::Literal(Value::Text(prefix))],
    };
    // ILIKE has the same casefold meaning even in a scalar projection, which
    // does not pass through WHERE normalization. Reuse the boolean IR owner.
    let expr = if casefold {
        normalize_bool_expr(expr, work)?
    } else {
        expr
    };

    Ok(if negated {
        charge_storage::<Expr>(1, work)?;
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        }
    } else {
        expr
    })
}

fn lower_sql_like_target_expr(
    expr: &SqlExpr,
    casefold: bool,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let target = lower_expr(expr, context.child(false), work)?;
    if casefold {
        charge_storage::<Expr>(1, work)?;
        return Ok(Expr::FunctionCall {
            function: Function::Lower,
            args: vec![target],
        });
    }

    Ok(target)
}

fn lower_sql_binary_expr(
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    let child = context
        .child(context.infer_here && matches!(op, SqlExprBinaryOp::And | SqlExprBinaryOp::Or));
    let lowered_left = lower_expr(left, child, work)?;
    let lowered_right = lower_expr(right, child, work)?;
    if matches!(left, SqlExpr::Field(_))
        && let Expr::Literal(literal) = &lowered_right
        && matches!(
            op,
            SqlExprBinaryOp::Add
                | SqlExprBinaryOp::Sub
                | SqlExprBinaryOp::Mul
                | SqlExprBinaryOp::Div
        )
    {
        validate_numeric_literal(literal)?;
    }
    charge_storage::<Expr>(2, work)?;
    let left = Box::new(lowered_left);
    let right = lowered_right;
    Ok(Expr::Binary {
        op: lower_sql_binary_op(op),
        left,
        right: Box::new(right),
    })
}

// This is SQL's dynamic-literal family, not Value's narrower numeric-coercion
// family. Typed fluent construction already requires NumericValue at compile time.
fn validate_numeric_literal(literal: &Value) -> Result<(), SqlLoweringError> {
    if matches!(
        literal,
        Value::Int64(_)
            | Value::Int128(_)
            | Value::Nat64(_)
            | Value::Nat128(_)
            | Value::U256(_)
            | Value::Decimal(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Duration(_)
            | Value::Timestamp(_)
            | Value::Date(_)
    ) {
        return Ok(());
    }
    Err(
        crate::db::QueryError::unsupported_projection(QueryProjectionCode::NumericLiteralRequired)
            .into(),
    )
}

const fn phase_allows_aggregate(phase: SqlExprPhase) -> bool {
    matches!(phase, SqlExprPhase::PostAggregate)
}

const fn phase_aggregate_error(phase: SqlExprPhase) -> SqlLoweringError {
    match phase {
        SqlExprPhase::Scalar => SqlLoweringError::unsupported_select_projection(),
        SqlExprPhase::Where | SqlExprPhase::PreAggregate => {
            SqlLoweringError::unsupported_aggregate_input_expressions()
        }
        SqlExprPhase::PostAggregate => SqlLoweringError::unsupported_aggregate_input_expressions(),
    }
}

const fn lower_sql_unary_op(op: SqlExprUnaryOp) -> UnaryOp {
    match op {
        SqlExprUnaryOp::Not => UnaryOp::Not,
    }
}

pub(in crate::db::sql::lowering) const fn lower_sql_binary_op(op: SqlExprBinaryOp) -> BinaryOp {
    match op {
        SqlExprBinaryOp::Or => BinaryOp::Or,
        SqlExprBinaryOp::And => BinaryOp::And,
        SqlExprBinaryOp::Eq => BinaryOp::Eq,
        SqlExprBinaryOp::Ne => BinaryOp::Ne,
        SqlExprBinaryOp::Lt => BinaryOp::Lt,
        SqlExprBinaryOp::Lte => BinaryOp::Lte,
        SqlExprBinaryOp::Gt => BinaryOp::Gt,
        SqlExprBinaryOp::Gte => BinaryOp::Gte,
        SqlExprBinaryOp::Add => BinaryOp::Add,
        SqlExprBinaryOp::Sub => BinaryOp::Sub,
        SqlExprBinaryOp::Mul => BinaryOp::Mul,
        SqlExprBinaryOp::Div => BinaryOp::Div,
    }
}

fn lower_sql_function_call(
    function: SqlScalarFunction,
    args: &[SqlExpr],
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    if function.uses_numeric_scale_special_case() {
        return lower_sql_numeric_scale_function_call(function, args, context, work);
    }

    let function = function.planner_function().ok_or_else(|| {
        SqlParseError::unsupported_feature(SqlFeatureCode::UnsupportedFunctionNamespace)
    })?;
    charge_storage::<Expr>(args.len(), work)?;
    let mut lowered = Vec::with_capacity(args.len());
    for arg in args {
        lowered.push(lower_expr(arg, context.child(false), work)?);
    }
    Ok(Expr::FunctionCall {
        function,
        args: lowered,
    })
}

fn lower_sql_numeric_scale_function_call(
    function: SqlScalarFunction,
    args: &[SqlExpr],
    context: SqlExprContext<'_>,
    work: &PreparationWork<'_>,
) -> Result<Expr, SqlLoweringError> {
    if !(1..=2).contains(&args.len()) {
        return Err(crate::db::QueryError::unsupported_sql_feature(
            SqlFeatureCode::NumericScaleFunctionArguments,
        )
        .into());
    }

    let input = lower_expr(&args[0], context.child(false), work)?;
    let scale = match args.get(1) {
        Some(scale) => {
            let lowered = lower_expr(scale, context.child(false), work)?;
            if let Expr::Literal(value) = &lowered {
                Expr::Literal(Value::Nat64(u64::from(
                    validate_numeric_scale_function_scale(value)?,
                )))
            } else {
                lowered
            }
        }
        None => Expr::Literal(Value::Nat64(0)),
    };

    charge_storage::<Expr>(2, work)?;
    Ok(Expr::FunctionCall {
        function: function.planner_function().ok_or_else(|| {
            SqlParseError::unsupported_feature(SqlFeatureCode::UnsupportedFunctionNamespace)
        })?,
        args: vec![input, scale],
    })
}

fn validate_numeric_scale_function_scale(scale: &Value) -> Result<u32, SqlLoweringError> {
    match scale {
        Value::Int64(value) => u32::try_from(*value).map_err(|_| {
            crate::db::QueryError::unsupported_sql_feature(
                SqlFeatureCode::NumericScaleFunctionArguments,
            )
            .into()
        }),
        Value::Nat64(value) => u32::try_from(*value).map_err(|_| {
            crate::db::QueryError::unsupported_sql_feature(
                SqlFeatureCode::NumericScaleFunctionArguments,
            )
            .into()
        }),
        _ => Err(crate::db::QueryError::unsupported_sql_feature(
            SqlFeatureCode::NumericScaleFunctionArguments,
        )
        .into()),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod construction_tests;
