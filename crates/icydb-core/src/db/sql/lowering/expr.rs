use crate::db::sql::lowering::{SqlLoweringError, aggregate::lower_aggregate_call};
use crate::{
    db::{
        query::{
            builder::NumericProjectionExpr,
            plan::expr::{BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp},
        },
        sql::parser::{SqlExpr, SqlExprBinaryOp, SqlExprUnaryOp, SqlScalarFunction},
    },
    value::Value,
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
    PreAggregate,
    PostAggregate,
}

// Lower one SQL expression tree into the canonical planner expression tree
// while enforcing the aggregate-admission rule for the owning clause phase.
pub(in crate::db::sql::lowering) fn lower_sql_expr(
    expr: &SqlExpr,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    match expr {
        SqlExpr::Field(field) => Ok(Expr::Field(FieldId::new(field.clone()))),
        SqlExpr::Aggregate(aggregate) => {
            if !phase_allows_aggregate(phase) {
                return Err(phase_aggregate_error(phase));
            }

            Ok(Expr::Aggregate(lower_aggregate_call(aggregate.clone())?))
        }
        SqlExpr::Literal(literal) => Ok(Expr::Literal(literal.clone())),
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => lower_sql_membership_expr(expr.as_ref(), values.as_slice(), *negated, phase),
        SqlExpr::NullTest { expr, negated } => Ok(Expr::FunctionCall {
            function: if *negated {
                Function::IsNotNull
            } else {
                Function::IsNull
            },
            args: vec![lower_sql_expr(expr.as_ref(), phase)?],
        }),
        SqlExpr::FunctionCall { function, args } => lower_sql_function_call(*function, args, phase),
        SqlExpr::Unary { op, expr } => Ok(Expr::Unary {
            op: lower_sql_unary_op(*op),
            expr: Box::new(lower_sql_expr(expr.as_ref(), phase)?),
        }),
        SqlExpr::Binary { op, left, right } => {
            lower_sql_binary_expr(*op, left.as_ref(), right.as_ref(), phase)
        }
        SqlExpr::Case { arms, else_expr } => Ok(Expr::Case {
            when_then_arms: arms
                .iter()
                .map(|arm| {
                    Ok(CaseWhenArm::new(
                        lower_sql_expr(&arm.condition, phase)?,
                        lower_sql_expr(&arm.result, phase)?,
                    ))
                })
                .collect::<Result<Vec<_>, SqlLoweringError>>()?,
            else_expr: Box::new(match else_expr.as_ref() {
                Some(else_expr) => lower_sql_expr(else_expr.as_ref(), phase)?,
                None => Expr::Literal(Value::Null),
            }),
        }),
    }
}

// Lower one parser-owned membership surface onto the existing boolean compare
// expression family so later WHERE compilation can still reuse the shipped
// normalized predicate path.
fn lower_sql_membership_expr(
    expr: &SqlExpr,
    values: &[Value],
    negated: bool,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    let Some((first, rest)) = values.split_first() else {
        unreachable!("parsed membership expression must keep at least one literal");
    };

    let compare_op = if negated {
        SqlExprBinaryOp::Ne
    } else {
        SqlExprBinaryOp::Eq
    };
    let join_op = if negated {
        SqlExprBinaryOp::And
    } else {
        SqlExprBinaryOp::Or
    };

    let mut lowered =
        lower_sql_binary_expr(compare_op, expr, &SqlExpr::Literal(first.clone()), phase)?;
    for value in rest {
        lowered = Expr::Binary {
            op: lower_sql_binary_op(join_op),
            left: Box::new(lowered),
            right: Box::new(lower_sql_binary_expr(
                compare_op,
                expr,
                &SqlExpr::Literal(value.clone()),
                phase,
            )?),
        };
    }

    Ok(lowered)
}

fn lower_sql_binary_expr(
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    if let (SqlExpr::Field(field), SqlExpr::Literal(literal)) = (left, right)
        && let Some(expr) = lower_field_literal_numeric_expr(op, field.as_str(), literal)?
    {
        return Ok(expr);
    }

    Ok(Expr::Binary {
        op: lower_sql_binary_op(op),
        left: Box::new(lower_sql_expr(left, phase)?),
        right: Box::new(lower_sql_expr(right, phase)?),
    })
}

fn lower_field_literal_numeric_expr(
    op: SqlExprBinaryOp,
    field: &str,
    literal: &Value,
) -> Result<Option<Expr>, SqlLoweringError> {
    let builder = match op {
        SqlExprBinaryOp::Add => Some(NumericProjectionExpr::add_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Sub => Some(NumericProjectionExpr::sub_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Mul => Some(NumericProjectionExpr::mul_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Div => Some(NumericProjectionExpr::div_value(
            field.to_string(),
            literal.clone(),
        )),
        SqlExprBinaryOp::Or
        | SqlExprBinaryOp::And
        | SqlExprBinaryOp::Eq
        | SqlExprBinaryOp::Ne
        | SqlExprBinaryOp::Lt
        | SqlExprBinaryOp::Lte
        | SqlExprBinaryOp::Gt
        | SqlExprBinaryOp::Gte => None,
    };

    builder
        .transpose()
        .map(|projection| projection.map(|projection| projection.expr().clone()))
        .map_err(SqlLoweringError::from)
}

// Return true when the SQL expression tree contains any aggregate leaf.
pub(in crate::db::sql::lowering) fn sql_expr_contains_aggregate(expr: &SqlExpr) -> bool {
    expr.contains_aggregate()
}

const fn phase_allows_aggregate(phase: SqlExprPhase) -> bool {
    matches!(phase, SqlExprPhase::PostAggregate)
}

fn phase_aggregate_error(phase: SqlExprPhase) -> SqlLoweringError {
    match phase {
        SqlExprPhase::Scalar => SqlLoweringError::unsupported_select_projection(),
        SqlExprPhase::PreAggregate => SqlLoweringError::unsupported_aggregate_input_expressions(),
        SqlExprPhase::PostAggregate => {
            unreachable!("post-aggregate lowering allows aggregate leaves")
        }
    }
}

const fn lower_sql_unary_op(op: SqlExprUnaryOp) -> UnaryOp {
    match op {
        SqlExprUnaryOp::Not => UnaryOp::Not,
    }
}

const fn lower_sql_binary_op(op: SqlExprBinaryOp) -> BinaryOp {
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

const fn lower_sql_scalar_function(function: SqlScalarFunction) -> Function {
    match function {
        SqlScalarFunction::Trim => Function::Trim,
        SqlScalarFunction::Ltrim => Function::Ltrim,
        SqlScalarFunction::Rtrim => Function::Rtrim,
        SqlScalarFunction::Round => Function::Round,
        SqlScalarFunction::Lower => Function::Lower,
        SqlScalarFunction::Upper => Function::Upper,
        SqlScalarFunction::Length => Function::Length,
        SqlScalarFunction::Left => Function::Left,
        SqlScalarFunction::Right => Function::Right,
        SqlScalarFunction::StartsWith => Function::StartsWith,
        SqlScalarFunction::EndsWith => Function::EndsWith,
        SqlScalarFunction::Contains => Function::Contains,
        SqlScalarFunction::Position => Function::Position,
        SqlScalarFunction::Replace => Function::Replace,
        SqlScalarFunction::Substring => Function::Substring,
    }
}

fn lower_sql_function_call(
    function: SqlScalarFunction,
    args: &[SqlExpr],
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    if matches!(function, SqlScalarFunction::Round) {
        return lower_sql_round_function_call(args, phase);
    }

    Ok(Expr::FunctionCall {
        function: lower_sql_scalar_function(function),
        args: args
            .iter()
            .map(|arg| lower_sql_expr(arg, phase))
            .collect::<Result<Vec<_>, SqlLoweringError>>()?,
    })
}

fn lower_sql_round_function_call(
    args: &[SqlExpr],
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    if !(1..=2).contains(&args.len()) {
        return Err(crate::db::QueryError::unsupported_query(format!(
            "ROUND(...) expects 1 or 2 args, found {}",
            args.len()
        ))
        .into());
    }

    let input = lower_sql_expr(&args[0], phase)?;
    let scale = match args.get(1) {
        Some(SqlExpr::Literal(scale)) => Expr::Literal(Value::Uint(u64::from(
            validate_round_projection_scale(scale.clone())?,
        ))),
        Some(other) => lower_sql_expr(other, phase)?,
        None => Expr::Literal(Value::Uint(0)),
    };

    Ok(Expr::FunctionCall {
        function: Function::Round,
        args: vec![input, scale],
    })
}

fn validate_round_projection_scale(scale: Value) -> Result<u32, SqlLoweringError> {
    match scale {
        Value::Int(value) => u32::try_from(value).map_err(|_| {
            crate::db::QueryError::unsupported_query(format!(
                "ROUND(...) requires non-negative integer scale, found {value}",
            ))
            .into()
        }),
        Value::Uint(value) => u32::try_from(value).map_err(|_| {
            crate::db::QueryError::unsupported_query(format!(
                "ROUND(...) scale exceeds supported integer range, found {value}",
            ))
            .into()
        }),
        other => Err(crate::db::QueryError::unsupported_query(format!(
            "ROUND(...) requires integer scale, found {other:?}",
        ))
        .into()),
    }
}
