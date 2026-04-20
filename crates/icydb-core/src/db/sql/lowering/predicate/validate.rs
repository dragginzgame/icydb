use crate::{
    db::{
        query::plan::expr::{BinaryOp, Expr, Function, UnaryOp},
        sql::lowering::SqlLoweringError,
    },
    value::Value,
};

// Validate one planner-owned boolean WHERE expression after shared SQL
// lowering. This owns clause admission only; it does not reshape semantics.
pub(super) fn validate_where_bool_expr(expr: &Expr) -> Result<(), SqlLoweringError> {
    match expr {
        // Keep bare field leaves admitted here so boolean-valued field
        // conditions can flow through CASE/NOT/AND/OR. Non-boolean fields still
        // fail closed later at normal schema validation once predicate
        // adaptation lowers them onto the canonical `field = TRUE/FALSE` seam.
        Expr::Field(_) => Ok(()),
        Expr::Literal(Value::Bool(_) | Value::Null) => Ok(()),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => validate_where_bool_expr(expr.as_ref()),
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            validate_where_bool_expr(left.as_ref())?;
            validate_where_bool_expr(right.as_ref())
        }
        Expr::Binary { op, left, right } => validate_where_bool_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => validate_where_bool_function_call(*function, args),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                validate_where_bool_expr(arm.condition())?;
                validate_where_bool_expr(arm.result())?;
            }

            validate_where_bool_expr(else_expr.as_ref())
        }
        #[cfg(test)]
        Expr::Alias { .. } => Err(SqlLoweringError::unsupported_where_expression()),
        Expr::Aggregate(_) | Expr::Literal(_) => {
            Err(SqlLoweringError::unsupported_where_expression())
        }
    }
}

// Validate one boolean comparison after shared SQL expression lowering so
// predicate compilation can stay total and structure-preserving.
fn validate_where_bool_compare_expr(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> Result<(), SqlLoweringError> {
    match op {
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
            if where_compare_operand_is_admitted(left)
                && where_compare_operand_is_admitted(right) =>
        {
            Ok(())
        }
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

// Validate one boolean function-call shell in WHERE after SQL expression
// lowering so the compiler can assume a bounded function family.
fn validate_where_bool_function_call(
    function: Function,
    args: &[Expr],
) -> Result<(), SqlLoweringError> {
    match function {
        Function::IsNull | Function::IsNotNull => match args {
            [arg] if where_null_test_operand_is_admitted(arg) => Ok(()),
            _ => Err(SqlLoweringError::unsupported_where_expression()),
        },
        Function::StartsWith | Function::EndsWith | Function::Contains => match args {
            [left, right]
                if where_compare_operand_is_admitted(left)
                    && where_compare_operand_is_admitted(right) =>
            {
                Ok(())
            }
            _ => Err(SqlLoweringError::unsupported_where_expression()),
        },
        _ => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

// Keep WHERE compare admission bounded to the reduced shipped operand family.
fn where_compare_operand_is_admitted(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
            args,
        } => args.iter().all(where_compare_operand_is_admitted),
        Expr::FunctionCall {
            function:
                Function::Coalesce
                | Function::NullIf
                | Function::Trim
                | Function::Ltrim
                | Function::Rtrim
                | Function::Abs
                | Function::Ceil
                | Function::Ceiling
                | Function::Floor
                | Function::Length
                | Function::Left
                | Function::Right
                | Function::Position
                | Function::Replace
                | Function::Substring
                | Function::Round,
            args,
        } => args.iter().all(where_compare_operand_is_admitted),
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            where_compare_operand_is_admitted(left.as_ref())
                && where_compare_operand_is_admitted(right.as_ref())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                validate_where_bool_expr(arm.condition()).is_ok()
                    && where_compare_operand_is_admitted(arm.result())
            }) && where_compare_operand_is_admitted(else_expr.as_ref())
        }
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::FunctionCall { .. }
        | Expr::Binary { .. } => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Keep null-test admission aligned with the shipped residual expression
// family instead of the older field-or-literal-only surface.
fn where_null_test_operand_is_admitted(expr: &Expr) -> bool {
    where_compare_operand_is_admitted(expr)
}
