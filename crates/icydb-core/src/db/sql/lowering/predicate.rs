use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{BinaryOp, Expr, Function, UnaryOp},
        sql::{
            lowering::{
                SqlLoweringError,
                expr::{SqlExprPhase, lower_sql_expr},
            },
            parser::SqlExpr,
        },
    },
    value::Value,
};

/// Lower one parser-owned SQL `WHERE` expression onto the runtime predicate
/// authority through the shared SQL-expression seam.
pub(in crate::db) fn lower_sql_where_expr(expr: &SqlExpr) -> Result<Predicate, SqlLoweringError> {
    let expr = lower_sql_expr(expr, SqlExprPhase::PreAggregate)?;
    let (when_true, _) = lower_expr_boolean_semantics(&expr)?;

    Ok(when_true)
}

// Convert one planner-owned boolean expression into the canonical runtime
// predicate tree while preserving the rows where the expression is definitely
// false. WHERE only keeps true rows, but NOT/CASE need false-set tracking so
// NULL does not collapse until the final predicate boundary.
fn lower_expr_boolean_semantics(expr: &Expr) -> Result<(Predicate, Predicate), SqlLoweringError> {
    match expr {
        Expr::Literal(Value::Bool(true)) => Ok((Predicate::True, Predicate::False)),
        Expr::Literal(Value::Bool(false)) => Ok((Predicate::False, Predicate::True)),
        Expr::Literal(Value::Null) => Ok((Predicate::False, Predicate::False)),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            let (when_true, when_false) = lower_expr_boolean_semantics(expr.as_ref())?;

            Ok((when_false, when_true))
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let (left_true, left_false) = lower_expr_boolean_semantics(left.as_ref())?;
            let (right_true, right_false) = lower_expr_boolean_semantics(right.as_ref())?;

            Ok((
                Predicate::And(vec![left_true, right_true]),
                Predicate::Or(vec![left_false, right_false]),
            ))
        }
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            let (left_true, left_false) = lower_expr_boolean_semantics(left.as_ref())?;
            let (right_true, right_false) = lower_expr_boolean_semantics(right.as_ref())?;

            Ok((
                Predicate::Or(vec![left_true, right_true]),
                Predicate::And(vec![left_false, right_false]),
            ))
        }
        Expr::Binary { op, left, right } => {
            lower_compare_expr_boolean_semantics(*op, left.as_ref(), right.as_ref())
        }
        Expr::FunctionCall { function, args } => {
            lower_boolean_function_call_semantics(*function, args)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => lower_case_expr_boolean_semantics(when_then_arms.as_slice(), else_expr.as_ref()),
        #[cfg(test)]
        Expr::Alias { .. } => Err(SqlLoweringError::unsupported_where_expression()),
        Expr::Field(_) | Expr::Aggregate(_) | Expr::Literal(_) => {
            Err(SqlLoweringError::unsupported_where_expression())
        }
    }
}

// CASE in WHERE stays mechanical: every branch lowers onto the same boolean
// predicate seam, and missing ELSE already normalized to explicit NULL before
// this boundary.
fn lower_case_expr_boolean_semantics(
    arms: &[crate::db::query::plan::expr::CaseWhenArm],
    else_expr: &Expr,
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    let (mut residual_true, mut residual_false) = lower_expr_boolean_semantics(else_expr)?;

    for arm in arms.iter().rev() {
        let (condition_true, _) = lower_expr_boolean_semantics(arm.condition())?;
        let (result_true, result_false) = lower_expr_boolean_semantics(arm.result())?;
        let skipped = Predicate::Not(Box::new(condition_true.clone()));

        residual_true = Predicate::Or(vec![
            Predicate::And(vec![condition_true.clone(), result_true]),
            Predicate::And(vec![skipped.clone(), residual_true]),
        ]);
        residual_false = Predicate::Or(vec![
            Predicate::And(vec![condition_true, result_false]),
            Predicate::And(vec![skipped, residual_false]),
        ]);
    }

    Ok((residual_true, residual_false))
}

fn lower_compare_expr_boolean_semantics(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    let compare_op = lower_compare_op(op)?;

    if let Some((when_true, when_false)) = lower_null_compare_semantics(compare_op, left, right) {
        return Ok((when_true, when_false));
    }
    if let Some((when_true, when_false)) = lower_field_compare_predicate(compare_op, left, right) {
        return Ok((when_true, when_false));
    }
    if let Some((when_true, when_false)) =
        lower_wrapped_text_compare_predicate(compare_op, left, right)
    {
        return Ok((when_true, when_false));
    }

    Err(SqlLoweringError::unsupported_where_expression())
}

const fn lower_compare_op(op: BinaryOp) -> Result<CompareOp, SqlLoweringError> {
    match op {
        BinaryOp::Eq => Ok(CompareOp::Eq),
        BinaryOp::Ne => Ok(CompareOp::Ne),
        BinaryOp::Lt => Ok(CompareOp::Lt),
        BinaryOp::Lte => Ok(CompareOp::Lte),
        BinaryOp::Gt => Ok(CompareOp::Gt),
        BinaryOp::Gte => Ok(CompareOp::Gte),
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

const fn lower_null_compare_semantics(
    _op: CompareOp,
    left: &Expr,
    right: &Expr,
) -> Option<(Predicate, Predicate)> {
    match (left, right) {
        (Expr::Field(field), Expr::Literal(Value::Null))
        | (Expr::Literal(Value::Null), Expr::Field(field)) => {
            let _ = field;
            Some((Predicate::False, Predicate::False))
        }
        (Expr::Literal(Value::Null), Expr::Literal(_))
        | (Expr::Literal(_), Expr::Literal(Value::Null)) => {
            Some((Predicate::False, Predicate::False))
        }
        _ => None,
    }
}

fn lower_field_compare_predicate(
    op: CompareOp,
    left: &Expr,
    right: &Expr,
) -> Option<(Predicate, Predicate)> {
    match (left, right) {
        (Expr::Field(field), Expr::Literal(value)) => {
            let when_true = Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op,
                value.clone(),
                compare_literal_coercion(op, value),
            ));

            Some((when_true.clone(), Predicate::Not(Box::new(when_true))))
        }
        (Expr::Literal(value), Expr::Field(field)) => {
            let when_true = Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op.flipped(),
                value.clone(),
                compare_literal_coercion(op.flipped(), value),
            ));

            Some((when_true.clone(), Predicate::Not(Box::new(when_true))))
        }
        (Expr::Field(left_field), Expr::Field(right_field)) => {
            let when_true = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                left_field.as_str().to_string(),
                op,
                right_field.as_str().to_string(),
                compare_field_coercion(op),
            ));

            Some((when_true.clone(), Predicate::Not(Box::new(when_true))))
        }
        _ => None,
    }
}

fn lower_wrapped_text_compare_predicate(
    op: CompareOp,
    left: &Expr,
    right: &Expr,
) -> Option<(Predicate, Predicate)> {
    if let Some((field, coercion, value)) = text_wrapped_field_literal_compare(left, right) {
        let when_true =
            Predicate::Compare(ComparePredicate::with_coercion(field, op, value, coercion));

        return Some((when_true.clone(), Predicate::Not(Box::new(when_true))));
    }
    if let Some((field, coercion, value)) = text_wrapped_field_literal_compare(right, left) {
        let when_true = Predicate::Compare(ComparePredicate::with_coercion(
            field,
            op.flipped(),
            value,
            coercion,
        ));

        return Some((when_true.clone(), Predicate::Not(Box::new(when_true))));
    }

    None
}

fn text_wrapped_field_literal_compare(
    left: &Expr,
    right: &Expr,
) -> Option<(String, CoercionId, Value)> {
    let Expr::FunctionCall { function, args } = left else {
        return None;
    };
    let [Expr::Field(field)] = args.as_slice() else {
        return None;
    };
    let Expr::Literal(Value::Text(value)) = right else {
        return None;
    };
    let coercion = match function {
        Function::Lower | Function::Upper => CoercionId::TextCasefold,
        _ => return None,
    };

    Some((
        field.as_str().to_string(),
        coercion,
        Value::Text(value.clone()),
    ))
}

fn lower_boolean_function_call_semantics(
    function: Function,
    args: &[Expr],
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    match function {
        Function::IsNull | Function::IsNotNull => {
            lower_null_test_function_semantics(function, args)
        }
        Function::StartsWith | Function::EndsWith => {
            lower_prefix_text_function_predicate(function, args)
        }
        Function::Contains => lower_contains_function_predicate(args),
        _ => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

fn lower_null_test_function_semantics(
    function: Function,
    args: &[Expr],
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    let [arg] = args else {
        return Err(SqlLoweringError::unsupported_where_expression());
    };

    match arg {
        Expr::Field(field) => {
            let is_null = Predicate::IsNull {
                field: field.as_str().to_string(),
            };
            let is_not_null = Predicate::IsNotNull {
                field: field.as_str().to_string(),
            };

            Ok(match function {
                Function::IsNull => (is_null, is_not_null),
                Function::IsNotNull => (is_not_null, is_null),
                _ => unreachable!("null-test helper called with non-null-test function"),
            })
        }
        Expr::Literal(Value::Null) => Ok(match function {
            Function::IsNull => (Predicate::True, Predicate::False),
            Function::IsNotNull => (Predicate::False, Predicate::True),
            _ => unreachable!("null-test helper called with non-null-test function"),
        }),
        Expr::Literal(_) => Ok(match function {
            Function::IsNull => (Predicate::False, Predicate::True),
            Function::IsNotNull => (Predicate::True, Predicate::False),
            _ => unreachable!("null-test helper called with non-null-test function"),
        }),
        _ => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

fn lower_prefix_text_function_predicate(
    function: Function,
    args: &[Expr],
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        return Err(SqlLoweringError::unsupported_where_expression());
    };
    let (field, coercion) = predicate_text_target(left)?;
    let op = match function {
        Function::StartsWith => CompareOp::StartsWith,
        Function::EndsWith => CompareOp::EndsWith,
        _ => unreachable!("prefix helper called with non-prefix text function"),
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::Text(value.clone()),
        coercion,
    ));

    Ok((when_true.clone(), Predicate::Not(Box::new(when_true))))
}

fn lower_contains_function_predicate(
    args: &[Expr],
) -> Result<(Predicate, Predicate), SqlLoweringError> {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        return Err(SqlLoweringError::unsupported_where_expression());
    };
    let (field, coercion) = predicate_text_target(left)?;

    let when_true = match coercion {
        CoercionId::Strict => Predicate::TextContains {
            field,
            value: Value::Text(value.clone()),
        },
        CoercionId::TextCasefold => Predicate::TextContainsCi {
            field,
            value: Value::Text(value.clone()),
        },
        CoercionId::NumericWiden | CoercionId::CollectionElement => {
            return Err(SqlLoweringError::unsupported_where_expression());
        }
    };

    Ok((when_true.clone(), Predicate::Not(Box::new(when_true))))
}

fn predicate_text_target(expr: &Expr) -> Result<(String, CoercionId), SqlLoweringError> {
    match expr {
        Expr::Field(field) => Ok((field.as_str().to_string(), CoercionId::Strict)),
        Expr::FunctionCall {
            function: Function::Lower | Function::Upper,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Ok((field.as_str().to_string(), CoercionId::TextCasefold)),
            _ => Err(SqlLoweringError::unsupported_where_expression()),
        },
        _ => Err(SqlLoweringError::unsupported_where_expression()),
    }
}

const fn compare_literal_coercion(op: CompareOp, value: &Value) -> CoercionId {
    match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            if matches!(value, Value::Text(_)) {
                CoercionId::Strict
            } else {
                CoercionId::NumericWiden
            }
        }
        _ => CoercionId::Strict,
    }
}

const fn compare_field_coercion(op: CompareOp) -> CoercionId {
    match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    }
}
