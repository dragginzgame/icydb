use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{BinaryOp, CaseWhenArm, Expr, Function, UnaryOp},
    },
    value::Value,
};

// Compile one normalized planner-owned boolean expression into the canonical
// runtime predicate tree. This stage is total over validated/normalized input.
pub(super) fn compile_where_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    debug_assert!(compile_ready_where_bool_expr(expr));

    compile_where_bool_truth_sets(expr).0
}

// Convert one normalized planner-owned boolean expression into the canonical
// runtime predicate tree while preserving the rows where the expression is
// definitely false. WHERE only keeps true rows, but NOT/CASE need false-set
// tracking so NULL does not collapse until the final predicate boundary.
fn compile_where_bool_truth_sets(expr: &Expr) -> (Predicate, Predicate) {
    debug_assert!(compile_ready_where_bool_expr(expr));

    match expr {
        Expr::Field(field) => compile_where_bool_field_truth_sets(field.as_str()),
        Expr::Literal(Value::Bool(true)) => (Predicate::True, Predicate::False),
        Expr::Literal(Value::Bool(false)) => (Predicate::False, Predicate::True),
        Expr::Literal(Value::Null) => (Predicate::False, Predicate::False),
        Expr::Literal(_) => {
            unreachable!("normalized WHERE compilation expects only boolean-context literals")
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            let (when_true, when_false) = compile_where_bool_truth_sets(expr.as_ref());

            (when_false, when_true)
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let (left_true, left_false) = compile_where_bool_truth_sets(left.as_ref());
            let (right_true, right_false) = compile_where_bool_truth_sets(right.as_ref());

            (
                Predicate::And(vec![left_true, right_true]),
                Predicate::Or(vec![left_false, right_false]),
            )
        }
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            let (left_true, left_false) = compile_where_bool_truth_sets(left.as_ref());
            let (right_true, right_false) = compile_where_bool_truth_sets(right.as_ref());

            (
                Predicate::Or(vec![left_true, right_true]),
                Predicate::And(vec![left_false, right_false]),
            )
        }
        Expr::Binary { op, left, right } => {
            compile_where_compare_truth_sets(*op, left.as_ref(), right.as_ref())
        }
        Expr::FunctionCall { function, args } => {
            compile_where_bool_function_truth_sets(*function, args)
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => compile_where_case_truth_sets(when_then_arms.as_slice(), else_expr.as_ref()),
        Expr::Aggregate(_) => {
            unreachable!("normalized WHERE compilation expects boolean-only expression shapes")
        }
        #[cfg(test)]
        Expr::Alias { .. } => {
            unreachable!("normalized WHERE compilation should never receive alias wrappers")
        }
    }
}

// CASE in WHERE stays purely structural here: every arm compiles onto the same
// boolean predicate seam without branch simplification or reordering.
fn compile_where_case_truth_sets(arms: &[CaseWhenArm], else_expr: &Expr) -> (Predicate, Predicate) {
    let (mut residual_true, mut residual_false) = compile_where_bool_truth_sets(else_expr);

    for arm in arms.iter().rev() {
        let (condition_true, _) = compile_where_bool_truth_sets(arm.condition());
        let (result_true, result_false) = compile_where_bool_truth_sets(arm.result());
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

    (residual_true, residual_false)
}

// Compile one normalized boolean field leaf onto the same canonical runtime
// predicate authority used by explicit `IS TRUE` / `IS FALSE` lowering.
fn compile_where_bool_field_truth_sets(field: &str) -> (Predicate, Predicate) {
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        field.to_string(),
        CompareOp::Eq,
        Value::Bool(true),
        CoercionId::Strict,
    ));
    let when_false = Predicate::Compare(ComparePredicate::with_coercion(
        field.to_string(),
        CompareOp::Eq,
        Value::Bool(false),
        CoercionId::Strict,
    ));

    (when_true, when_false)
}

// Compile one normalized compare shell directly into the runtime predicate
// authority and derive its false-set mechanically.
fn compile_where_compare_truth_sets(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> (Predicate, Predicate) {
    let when_true = compile_where_compare_predicate(lower_compare_op(op), left, right);

    (
        when_true.clone(),
        match when_true {
            Predicate::False => Predicate::False,
            predicate => Predicate::Not(Box::new(predicate)),
        },
    )
}

// Lower one normalized planner compare operator into the runtime predicate
// compare operator taxonomy.
fn lower_compare_op(op: BinaryOp) -> CompareOp {
    match op {
        BinaryOp::Eq => CompareOp::Eq,
        BinaryOp::Ne => CompareOp::Ne,
        BinaryOp::Lt => CompareOp::Lt,
        BinaryOp::Lte => CompareOp::Lte,
        BinaryOp::Gt => CompareOp::Gt,
        BinaryOp::Gte => CompareOp::Gte,
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => {
            unreachable!("normalized WHERE compilation only lowers compare operators")
        }
    }
}

// Compile one normalized compare shell into the runtime predicate form without
// any semantic reshaping.
fn compile_where_compare_predicate(op: CompareOp, left: &Expr, right: &Expr) -> Predicate {
    match (left, right) {
        (Expr::Field(_) | Expr::Literal(_), Expr::Literal(Value::Null))
        | (Expr::Literal(Value::Null), Expr::Field(_) | Expr::Literal(_)) => Predicate::False,
        (Expr::Field(field), Expr::Literal(value)) => {
            Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op,
                value.clone(),
                compare_literal_coercion(op, value),
            ))
        }
        (Expr::Field(left_field), Expr::Field(right_field)) => {
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                left_field.as_str().to_string(),
                op,
                right_field.as_str().to_string(),
                compare_field_coercion(op),
            ))
        }
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            },
            Expr::Literal(Value::Text(value)),
        ) => match args.as_slice() {
            [Expr::Field(field)] => Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op,
                Value::Text(value.clone()),
                CoercionId::TextCasefold,
            )),
            _ => unreachable!("normalized WHERE compilation expects LOWER(field) compare wrappers"),
        },
        _ => unreachable!("normalized WHERE compilation expects canonical compare operands"),
    }
}

// Compile one normalized boolean function shell into true/false predicate sets.
fn compile_where_bool_function_truth_sets(
    function: Function,
    args: &[Expr],
) -> (Predicate, Predicate) {
    match function {
        Function::IsNull | Function::IsNotNull => {
            compile_where_null_test_function_truth_sets(function, args)
        }
        Function::StartsWith | Function::EndsWith => {
            compile_where_prefix_text_function_truth_sets(function, args)
        }
        Function::Contains => compile_where_contains_function_truth_sets(args),
        _ => unreachable!("normalized WHERE compilation expects only admitted boolean functions"),
    }
}

// Compile one normalized null-test shell without interpreting additional
// semantics beyond the already-normalized operand shape.
fn compile_where_null_test_function_truth_sets(
    function: Function,
    args: &[Expr],
) -> (Predicate, Predicate) {
    let [arg] = args else {
        unreachable!("normalized WHERE null tests keep one operand")
    };

    match arg {
        Expr::Field(field) => {
            let is_null = Predicate::IsNull {
                field: field.as_str().to_string(),
            };
            let is_not_null = Predicate::IsNotNull {
                field: field.as_str().to_string(),
            };

            match function {
                Function::IsNull => (is_null, is_not_null),
                Function::IsNotNull => (is_not_null, is_null),
                _ => unreachable!("null-test compiler called with non-null-test function"),
            }
        }
        Expr::Literal(Value::Null) => match function {
            Function::IsNull => (Predicate::True, Predicate::False),
            Function::IsNotNull => (Predicate::False, Predicate::True),
            _ => unreachable!("null-test compiler called with non-null-test function"),
        },
        Expr::Literal(_) => match function {
            Function::IsNull => (Predicate::False, Predicate::True),
            Function::IsNotNull => (Predicate::True, Predicate::False),
            _ => unreachable!("null-test compiler called with non-null-test function"),
        },
        _ => unreachable!("normalized WHERE null tests expect field/literal operands"),
    }
}

// Compile one normalized prefix text shell without rewriting target wrappers.
fn compile_where_prefix_text_function_truth_sets(
    function: Function,
    args: &[Expr],
) -> (Predicate, Predicate) {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        unreachable!("normalized WHERE prefix text predicates keep field/text operands")
    };
    let (field, coercion) = compile_where_text_target(left);
    let op = match function {
        Function::StartsWith => CompareOp::StartsWith,
        Function::EndsWith => CompareOp::EndsWith,
        _ => unreachable!("prefix compiler called with non-prefix text function"),
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::Text(value.clone()),
        coercion,
    ));

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

// Compile one normalized contains shell without reinterpreting wrapper shape.
fn compile_where_contains_function_truth_sets(args: &[Expr]) -> (Predicate, Predicate) {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        unreachable!("normalized WHERE contains predicates keep field/text operands")
    };
    let (field, coercion) = compile_where_text_target(left);

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
            unreachable!("normalized WHERE contains predicates only compile text coercions");
        }
    };

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

// Compile one normalized text target into the runtime field/coercion pair.
fn compile_where_text_target(expr: &Expr) -> (String, CoercionId) {
    match expr {
        Expr::Field(field) => (field.as_str().to_string(), CoercionId::Strict),
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => (field.as_str().to_string(), CoercionId::TextCasefold),
            _ => unreachable!("normalized WHERE text targets only compile LOWER(field) wrappers"),
        },
        _ => unreachable!("normalized WHERE text targets only compile canonical field wrappers"),
    }
}

// Report whether one expression satisfies the compile-time normalized contract.
fn compile_ready_where_bool_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) => true,
        Expr::Literal(Value::Bool(_) | Value::Null) => true,
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            !matches!(
                expr.as_ref(),
                Expr::Unary {
                    op: UnaryOp::Not,
                    ..
                }
            ) && compile_ready_where_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            compile_ready_where_bool_expr(left.as_ref())
                && compile_ready_where_bool_expr(right.as_ref())
        }
        Expr::Binary { op, left, right } => compile_ready_where_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            compile_ready_where_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                compile_ready_where_bool_expr(arm.condition())
                    && compile_ready_where_bool_expr(arm.result())
            }) && compile_ready_where_bool_expr(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Report whether one compare shell is already in the canonical compiled shape.
fn compile_ready_where_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match op {
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => match (left, right) {
            (
                Expr::Literal(_),
                Expr::Field(_)
                | Expr::FunctionCall {
                    function: Function::Lower,
                    ..
                },
            ) => false,
            (Expr::Field(left_field), Expr::Field(right_field))
                if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
            {
                false
            }
            _ => {
                compile_ready_where_compare_operand(left)
                    && compile_ready_where_compare_operand(right)
            }
        },
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => false,
    }
}

// Report whether one compare operand is already in the canonical field/literal
// or LOWER(field) wrapper shape.
fn compile_ready_where_compare_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => matches!(args.as_slice(), [Expr::Field(_)]),
        Expr::FunctionCall {
            function: Function::Upper,
            ..
        } => false,
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::Binary { .. }
        | Expr::Case { .. }
        | Expr::FunctionCall { .. } => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Report whether one boolean function shell is already in compile-ready form.
fn compile_ready_where_bool_function_call(function: Function, args: &[Expr]) -> bool {
    match function {
        Function::IsNull | Function::IsNotNull => {
            matches!(args, [Expr::Field(_) | Expr::Literal(_)])
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            matches!(args, [left, Expr::Literal(Value::Text(_))] if compile_ready_where_compare_operand(left))
        }
        _ => false,
    }
}

// Choose compare coercion for field/literal compare predicates.
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

// Choose compare coercion for field/field compare predicates.
const fn compare_field_coercion(op: CompareOp) -> CoercionId {
    match op {
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::query::plan::expr::{BinaryOp, Expr, FieldId},
        value::Value,
    };

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn compile_where_bool_expr_requires_normalized_shape() {
        let expr = Expr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Literal(Value::Int(5))),
            right: Box::new(Expr::Field(FieldId::new("age"))),
        };

        let _ = super::compile_where_bool_expr_to_predicate(&expr);
    }
}
