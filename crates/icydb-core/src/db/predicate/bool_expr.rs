#[cfg(test)]
use crate::db::query::plan::expr::FieldId;
use crate::{
    db::{
        access::canonical::canonicalize_value_set,
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{BinaryOp, CaseWhenArm, Expr, Function, UnaryOp},
    },
    value::Value,
};

/// Canonicalize one predicate by routing it through the shared planner-owned
/// boolean expression seam before rebuilding the runtime predicate form.
#[cfg(test)]
#[must_use]
pub(in crate::db) fn canonicalize_predicate_via_bool_expr(predicate: Predicate) -> Predicate {
    let expr = predicate_to_bool_expr(&predicate);
    let expr = normalize_bool_expr(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    crate::db::predicate::normalize(&compile_bool_expr_to_predicate(&expr))
}

/// Normalize one planner-owned boolean expression without changing
/// three-valued semantics inside subexpressions.
#[must_use]
pub(in crate::db) fn normalize_bool_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match normalize_bool_expr(*expr) {
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => *expr,
            Expr::Literal(Value::Bool(value)) => Expr::Literal(Value::Bool(!value)),
            Expr::Literal(Value::Null) => Expr::Literal(Value::Null),
            expr => Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            },
        },
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(normalize_bool_expr(*left)),
            right: Box::new(normalize_bool_expr(*right)),
        },
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(normalize_bool_expr(*left)),
            right: Box::new(normalize_bool_expr(*right)),
        },
        Expr::Binary { op, left, right } => normalize_bool_compare_expr(
            op,
            normalize_bool_compare_operand(*left),
            normalize_bool_compare_operand(*right),
        ),
        Expr::FunctionCall { function, args } => normalize_bool_function_call(function, args),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        normalize_bool_expr(arm.condition().clone()),
                        normalize_bool_expr(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_bool_expr(*else_expr)),
        },
        other => other,
    }
}

/// Report whether one boolean expression is already in the canonical
/// normalized shape required by predicate compilation.
#[must_use]
pub(in crate::db) fn is_normalized_bool_expr(expr: &Expr) -> bool {
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
            ) && is_normalized_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => is_normalized_bool_expr(left.as_ref()) && is_normalized_bool_expr(right.as_ref()),
        Expr::Binary { op, left, right } => is_normalized_bool_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            is_normalized_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                is_normalized_bool_expr(arm.condition()) && is_normalized_bool_expr(arm.result())
            }) && is_normalized_bool_expr(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

/// Compile one normalized planner-owned boolean expression into the canonical
/// runtime predicate tree.
#[must_use]
pub(in crate::db) fn compile_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    debug_assert!(
        compile_ready_bool_expr(expr),
        "normalized boolean expression"
    );

    if let Some(predicate) = collapse_membership_bool_expr(expr) {
        return predicate;
    }

    compile_bool_truth_sets(expr).0
}

/// Return whether one normalized boolean expression still fits the legacy
/// predicate compilation contract instead of requiring expression-only
/// residual evaluation.
#[must_use]
pub(in crate::db) fn bool_expr_supports_predicate_compilation(expr: &Expr) -> bool {
    compile_ready_bool_expr(expr)
}

// Convert one predicate tree into one planner-owned boolean expression.
#[cfg(test)]
fn predicate_to_bool_expr(predicate: &Predicate) -> Expr {
    match predicate {
        Predicate::True => Expr::Literal(Value::Bool(true)),
        Predicate::False => Expr::Literal(Value::Bool(false)),
        Predicate::And(children) => combine_bool_chain(BinaryOp::And, children),
        Predicate::Or(children) => combine_bool_chain(BinaryOp::Or, children),
        Predicate::Not(inner) => Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(predicate_to_bool_expr(inner)),
        },
        Predicate::Compare(compare) => compare_predicate_to_bool_expr(compare),
        Predicate::CompareFields(compare) => compare_fields_predicate_to_bool_expr(compare),
        Predicate::IsNull { field } => field_function_expr(Function::IsNull, field.as_str()),
        Predicate::IsNotNull { field } => field_function_expr(Function::IsNotNull, field.as_str()),
        Predicate::IsMissing { field } => field_function_expr(Function::IsMissing, field.as_str()),
        Predicate::IsEmpty { field } => field_function_expr(Function::IsEmpty, field.as_str()),
        Predicate::IsNotEmpty { field } => {
            field_function_expr(Function::IsNotEmpty, field.as_str())
        }
        Predicate::TextContains { field, value } => text_function_expr(
            Function::Contains,
            Expr::Field(FieldId::new(field.clone())),
            value.clone(),
        ),
        Predicate::TextContainsCi { field, value } => text_function_expr(
            Function::Contains,
            casefold_field_expr(field.as_str(), CoercionId::TextCasefold),
            value.clone(),
        ),
    }
}

// Build one canonical boolean chain, preserving empty-chain constants.
#[cfg(test)]
fn combine_bool_chain(op: BinaryOp, children: &[Predicate]) -> Expr {
    let mut children = children.iter().map(predicate_to_bool_expr);
    let Some(first) = children.next() else {
        return Expr::Literal(Value::Bool(matches!(op, BinaryOp::And)));
    };

    children.fold(first, |left, right| Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

// Convert one compare predicate into one planner-owned canonical boolean expression.
#[cfg(test)]
fn compare_predicate_to_bool_expr(compare: &ComparePredicate) -> Expr {
    match compare.op() {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => Expr::Binary {
            op: binary_compare_op(compare.op()),
            left: Box::new(casefold_field_expr(
                compare.field(),
                compare.coercion().id(),
            )),
            right: Box::new(Expr::Literal(compare.value().clone())),
        },
        CompareOp::In | CompareOp::NotIn => membership_compare_predicate_to_bool_expr(compare),
        CompareOp::Contains => Expr::FunctionCall {
            function: Function::CollectionContains,
            args: vec![
                Expr::Field(FieldId::new(compare.field().to_owned())),
                Expr::Literal(compare.value().clone()),
            ],
        },
        CompareOp::StartsWith => text_function_expr(
            Function::StartsWith,
            casefold_field_expr(compare.field(), compare.coercion().id()),
            compare.value().clone(),
        ),
        CompareOp::EndsWith => text_function_expr(
            Function::EndsWith,
            casefold_field_expr(compare.field(), compare.coercion().id()),
            compare.value().clone(),
        ),
    }
}

// Convert one field-to-field compare predicate into one planner-owned boolean expression.
#[cfg(test)]
fn compare_fields_predicate_to_bool_expr(compare: &CompareFieldsPredicate) -> Expr {
    Expr::Binary {
        op: binary_compare_op(compare.op()),
        left: Box::new(casefold_field_expr(
            compare.left_field.as_str(),
            compare.coercion.id(),
        )),
        right: Box::new(casefold_field_expr(
            compare.right_field.as_str(),
            compare.coercion.id(),
        )),
    }
}

// Convert one `IN`/`NOT IN` compare into the canonical OR-of-EQ / AND-of-NE
// boolean shape consumed by shared membership collapse.
#[cfg(test)]
fn membership_compare_predicate_to_bool_expr(compare: &ComparePredicate) -> Expr {
    let values = match compare.value() {
        Value::List(values) => values.as_slice(),
        _ => return Expr::Literal(Value::Bool(matches!(compare.op(), CompareOp::NotIn))),
    };

    let compare_op = match compare.op() {
        CompareOp::In => BinaryOp::Eq,
        CompareOp::NotIn => BinaryOp::Ne,
        _ => unreachable!("membership converter called with non-membership compare"),
    };
    let join_op = match compare.op() {
        CompareOp::In => BinaryOp::Or,
        CompareOp::NotIn => BinaryOp::And,
        _ => unreachable!("membership converter called with non-membership compare"),
    };

    let mut values = values.iter();
    let Some(first) = values.next() else {
        return Expr::Literal(Value::Bool(matches!(compare.op(), CompareOp::NotIn)));
    };

    let field = casefold_field_expr(compare.field(), compare.coercion().id());
    let mut expr = Expr::Binary {
        op: compare_op,
        left: Box::new(field.clone()),
        right: Box::new(Expr::Literal(first.clone())),
    };

    for value in values {
        expr = Expr::Binary {
            op: join_op,
            left: Box::new(expr),
            right: Box::new(Expr::Binary {
                op: compare_op,
                left: Box::new(field.clone()),
                right: Box::new(Expr::Literal(value.clone())),
            }),
        };
    }

    expr
}

// Build one field-targeted boolean function shell.
#[cfg(test)]
fn field_function_expr(function: Function, field: &str) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![Expr::Field(FieldId::new(field.to_owned()))],
    }
}

// Build one text-targeted boolean function shell.
#[cfg(test)]
fn text_function_expr(function: Function, left: Expr, value: Value) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![left, Expr::Literal(value)],
    }
}

// Wrap one field in LOWER(...) only for casefold coercion.
#[cfg(test)]
fn casefold_field_expr(field: &str, coercion: CoercionId) -> Expr {
    match coercion {
        CoercionId::TextCasefold => Expr::FunctionCall {
            function: Function::Lower,
            args: vec![Expr::Field(FieldId::new(field.to_owned()))],
        },
        CoercionId::Strict | CoercionId::NumericWiden | CoercionId::CollectionElement => {
            Expr::Field(FieldId::new(field.to_owned()))
        }
    }
}

// Convert one compare operator into the planner-owned binary compare operator.
#[cfg(test)]
fn binary_compare_op(op: CompareOp) -> BinaryOp {
    match op {
        CompareOp::Eq => BinaryOp::Eq,
        CompareOp::Ne => BinaryOp::Ne,
        CompareOp::Lt => BinaryOp::Lt,
        CompareOp::Lte => BinaryOp::Lte,
        CompareOp::Gt => BinaryOp::Gt,
        CompareOp::Gte => BinaryOp::Gte,
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => {
            unreachable!("non-binary compare operator cannot map directly onto BinaryOp")
        }
    }
}

fn normalize_bool_compare_expr(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    match (&left, &right) {
        (
            Expr::Literal(_),
            Expr::Field(_)
            | Expr::FunctionCall {
                function: Function::Lower,
                ..
            },
        ) => Expr::Binary {
            op: flip_bool_compare_op(op),
            left: Box::new(right),
            right: Box::new(left),
        },
        (Expr::Field(left_field), Expr::Field(right_field))
            if matches!(op, BinaryOp::Eq | BinaryOp::Ne) && left_field < right_field =>
        {
            Expr::Binary {
                op,
                left: Box::new(right),
                right: Box::new(left),
            }
        }
        _ => Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn normalize_bool_compare_operand(expr: Expr) -> Expr {
    match expr {
        Expr::FunctionCall {
            function: Function::Upper | Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Expr::FunctionCall {
                function: Function::Lower,
                args: vec![Expr::Field(field.clone())],
            },
            _ => Expr::FunctionCall {
                function: Function::Lower,
                args: args
                    .into_iter()
                    .map(normalize_bool_compare_operand)
                    .collect(),
            },
        },
        Expr::FunctionCall { function, args } => Expr::FunctionCall {
            function,
            args: args
                .into_iter()
                .map(normalize_bool_compare_operand)
                .collect(),
        },
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            Expr::Binary {
                op,
                left: Box::new(normalize_bool_compare_operand(*left)),
                right: Box::new(normalize_bool_compare_operand(*right)),
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    CaseWhenArm::new(
                        normalize_bool_expr(arm.condition().clone()),
                        normalize_bool_compare_operand(arm.result().clone()),
                    )
                })
                .collect(),
            else_expr: Box::new(normalize_bool_compare_operand(*else_expr)),
        },
        expr => expr,
    }
}

fn normalize_bool_function_call(function: Function, args: Vec<Expr>) -> Expr {
    match function {
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            let [left, right] = <[Expr; 2]>::try_from(args)
                .expect("validated boolean text predicate should keep two arguments");

            Expr::FunctionCall {
                function,
                args: vec![normalize_bool_compare_operand(left), right],
            }
        }
        _ => Expr::FunctionCall { function, args },
    }
}

fn is_normalized_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match (left, right) {
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
        _ => is_normalized_bool_compare_operand(left) && is_normalized_bool_compare_operand(right),
    }
}

fn is_normalized_bool_compare_operand(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) | Expr::Literal(_) => true,
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => matches!(args.as_slice(), [arg] if is_normalized_bool_compare_operand(arg)),
        Expr::FunctionCall {
            function:
                Function::Coalesce
                | Function::NullIf
                | Function::Abs
                | Function::Ceil
                | Function::Ceiling
                | Function::Floor
                | Function::Round,
            args,
        } => args.iter().all(is_normalized_bool_compare_operand),
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            is_normalized_bool_compare_operand(left.as_ref())
                && is_normalized_bool_compare_operand(right.as_ref())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                is_normalized_bool_expr(arm.condition())
                    && is_normalized_bool_compare_operand(arm.result())
            }) && is_normalized_bool_compare_operand(else_expr.as_ref())
        }
        Expr::FunctionCall {
            function: Function::Upper,
            ..
        } => false,
        Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::Binary { .. }
        | Expr::FunctionCall { .. } => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

fn is_normalized_bool_function_call(function: Function, args: &[Expr]) -> bool {
    match function {
        Function::IsNull | Function::IsNotNull => {
            matches!(args, [Expr::Field(_) | Expr::Literal(_)])
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            matches!(args, [left, Expr::Literal(Value::Text(_))] if is_normalized_bool_compare_operand(left))
        }
        Function::IsMissing | Function::IsEmpty | Function::IsNotEmpty => {
            matches!(args, [Expr::Field(_)])
        }
        Function::CollectionContains => matches!(args, [Expr::Field(_), Expr::Literal(_)]),
        _ => false,
    }
}

const fn flip_bool_compare_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::Ne => BinaryOp::Ne,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => op,
    }
}

fn collapse_membership_bool_expr(expr: &Expr) -> Option<Predicate> {
    match expr {
        Expr::Binary {
            op: BinaryOp::Or, ..
        } => collapse_same_field_compare_chain(expr, BinaryOp::Or, BinaryOp::Eq, CompareOp::In),
        Expr::Binary {
            op: BinaryOp::And, ..
        } => collapse_same_field_compare_chain(expr, BinaryOp::And, BinaryOp::Ne, CompareOp::NotIn),
        Expr::Field(_)
        | Expr::Literal(_)
        | Expr::Unary { .. }
        | Expr::Aggregate(_)
        | Expr::FunctionCall { .. }
        | Expr::Case { .. }
        | Expr::Binary { .. } => None,
        #[cfg(test)]
        Expr::Alias { .. } => None,
    }
}

fn collapse_same_field_compare_chain(
    expr: &Expr,
    join_op: BinaryOp,
    compare_op: BinaryOp,
    target_op: CompareOp,
) -> Option<Predicate> {
    let mut leaves = Vec::new();
    collect_compare_chain(expr, join_op, &mut leaves)?;

    let mut field = None;
    let mut coercion = None;
    let mut values = Vec::with_capacity(leaves.len());

    for leaf in leaves {
        let (leaf_field, leaf_value, leaf_coercion) = membership_compare_leaf(leaf, compare_op)?;
        if let Some(current) = field {
            if current != leaf_field {
                return None;
            }
        } else {
            field = Some(leaf_field);
        }
        if let Some(current) = coercion {
            if current != leaf_coercion {
                return None;
            }
        } else {
            coercion = Some(leaf_coercion);
        }

        values.push(leaf_value);
    }

    canonicalize_value_set(&mut values);

    Some(Predicate::Compare(ComparePredicate::with_coercion(
        field?.to_string(),
        target_op,
        Value::List(values),
        coercion?,
    )))
}

fn collect_compare_chain<'a>(
    expr: &'a Expr,
    join_op: BinaryOp,
    out: &mut Vec<&'a Expr>,
) -> Option<()> {
    match expr {
        Expr::Binary { op, left, right } if *op == join_op => {
            collect_compare_chain(left.as_ref(), join_op, out)?;
            collect_compare_chain(right.as_ref(), join_op, out)
        }
        Expr::Binary { .. } => {
            out.push(expr);
            Some(())
        }
        Expr::Field(_)
        | Expr::Literal(_)
        | Expr::Unary { .. }
        | Expr::Aggregate(_)
        | Expr::FunctionCall { .. }
        | Expr::Case { .. } => None,
        #[cfg(test)]
        Expr::Alias { .. } => None,
    }
}

fn membership_compare_leaf(expr: &Expr, compare_op: BinaryOp) -> Option<(&str, Value, CoercionId)> {
    let Expr::Binary { op, left, right } = expr else {
        return None;
    };
    if *op != compare_op {
        return None;
    }

    match (left.as_ref(), right.as_ref()) {
        (Expr::Field(field), Expr::Literal(value)) if membership_value_is_in_safe(value) => Some((
            field.as_str(),
            value.clone(),
            compare_literal_coercion(lower_compare_op(*op), value),
        )),
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            },
            Expr::Literal(Value::Text(value)),
        ) => match args.as_slice() {
            [Expr::Field(field)] => Some((
                field.as_str(),
                Value::Text(value.clone()),
                CoercionId::TextCasefold,
            )),
            _ => None,
        },
        _ => None,
    }
}

const fn membership_value_is_in_safe(value: &Value) -> bool {
    !matches!(value, Value::List(_) | Value::Map(_))
}

fn compile_bool_truth_sets(expr: &Expr) -> (Predicate, Predicate) {
    debug_assert!(compile_ready_bool_expr(expr));

    match expr {
        Expr::Field(field) => compile_bool_field_truth_sets(field.as_str()),
        Expr::Literal(Value::Bool(true)) => (Predicate::True, Predicate::False),
        Expr::Literal(Value::Bool(false)) => (Predicate::False, Predicate::True),
        Expr::Literal(Value::Null) => (Predicate::False, Predicate::False),
        Expr::Literal(_) => {
            unreachable!("boolean compilation expects only boolean-context literals")
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            let (when_true, when_false) = compile_bool_truth_sets(expr.as_ref());
            (when_false, when_true)
        }
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let (left_true, left_false) = compile_bool_truth_sets(left.as_ref());
            let (right_true, right_false) = compile_bool_truth_sets(right.as_ref());

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
            let (left_true, left_false) = compile_bool_truth_sets(left.as_ref());
            let (right_true, right_false) = compile_bool_truth_sets(right.as_ref());

            (
                Predicate::Or(vec![left_true, right_true]),
                Predicate::And(vec![left_false, right_false]),
            )
        }
        Expr::Binary { op, left, right } => {
            compile_bool_compare_truth_sets(*op, left.as_ref(), right.as_ref())
        }
        Expr::FunctionCall { function, args } => compile_bool_function_truth_sets(*function, args),
        Expr::Case {
            when_then_arms,
            else_expr,
        } => compile_bool_case_truth_sets(when_then_arms.as_slice(), else_expr.as_ref()),
        Expr::Aggregate(_) => {
            unreachable!("boolean compilation expects boolean-only expression shapes")
        }
        #[cfg(test)]
        Expr::Alias { .. } => {
            unreachable!("boolean compilation should never receive alias wrappers")
        }
    }
}

fn compile_bool_case_truth_sets(arms: &[CaseWhenArm], else_expr: &Expr) -> (Predicate, Predicate) {
    let (mut residual_true, mut residual_false) = compile_bool_truth_sets(else_expr);

    for arm in arms.iter().rev() {
        let (condition_true, _) = compile_bool_truth_sets(arm.condition());
        let (result_true, result_false) = compile_bool_truth_sets(arm.result());
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

fn compile_bool_field_truth_sets(field: &str) -> (Predicate, Predicate) {
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

fn compile_bool_compare_truth_sets(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> (Predicate, Predicate) {
    if matches!(left, Expr::Literal(Value::Null)) || matches!(right, Expr::Literal(Value::Null)) {
        return (Predicate::False, Predicate::False);
    }

    let when_true = compile_bool_compare_expr(op, left, right);

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

fn compile_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> Predicate {
    let op = lower_compare_op(op);

    match (left, right) {
        (Expr::Field(field), Expr::Literal(value)) => {
            Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op,
                value.clone(),
                compare_literal_coercion(op, value),
            ))
        }
        (Expr::Literal(value), Expr::Field(field)) => {
            Predicate::Compare(ComparePredicate::with_coercion(
                field.as_str().to_string(),
                op.flipped(),
                value.clone(),
                compare_literal_coercion(op.flipped(), value),
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
            _ => unreachable!("boolean compilation expects LOWER(field) compare wrappers"),
        },
        _ => unreachable!("boolean compilation expects canonical compare operands"),
    }
}

fn compile_bool_function_truth_sets(function: Function, args: &[Expr]) -> (Predicate, Predicate) {
    match function {
        Function::IsNull | Function::IsNotNull => {
            compile_bool_null_test_function_truth_sets(function, args)
        }
        Function::StartsWith | Function::EndsWith => {
            compile_bool_prefix_text_function_truth_sets(function, args)
        }
        Function::Contains => compile_bool_contains_function_truth_sets(args),
        Function::IsMissing => {
            compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsMissing {
                field: field.to_string(),
            })
        }
        Function::IsEmpty => {
            compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsEmpty {
                field: field.to_string(),
            })
        }
        Function::IsNotEmpty => {
            compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsNotEmpty {
                field: field.to_string(),
            })
        }
        Function::CollectionContains => compile_bool_collection_contains_truth_sets(args),
        _ => unreachable!("boolean compilation expects only admitted boolean functions"),
    }
}

fn compile_bool_null_test_function_truth_sets(
    function: Function,
    args: &[Expr],
) -> (Predicate, Predicate) {
    let [arg] = args else {
        unreachable!("boolean null tests keep one operand")
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
        _ => unreachable!("boolean null tests expect field/literal operands"),
    }
}

fn compile_bool_prefix_text_function_truth_sets(
    function: Function,
    args: &[Expr],
) -> (Predicate, Predicate) {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        unreachable!("boolean prefix text predicates keep field/text operands")
    };
    let (field, coercion) = compile_bool_text_target(left);
    let op = match function {
        Function::StartsWith => CompareOp::StartsWith,
        Function::EndsWith => CompareOp::EndsWith,
        _ => unreachable!("prefix compiler called with non-prefix scalar function"),
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        field,
        op,
        Value::Text(value.clone()),
        coercion,
    ));

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

fn compile_bool_contains_function_truth_sets(args: &[Expr]) -> (Predicate, Predicate) {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        unreachable!("boolean contains predicates keep field/text operands")
    };
    let (field, coercion) = compile_bool_text_target(left);

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
            unreachable!("boolean contains predicates only compile text coercions");
        }
    };

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

fn compile_bool_field_predicate_truth_sets(
    args: &[Expr],
    build: impl FnOnce(&str) -> Predicate,
) -> (Predicate, Predicate) {
    let [Expr::Field(field)] = args else {
        unreachable!("field-only boolean function expects one field argument")
    };
    let when_true = build(field.as_str());

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

fn compile_bool_collection_contains_truth_sets(args: &[Expr]) -> (Predicate, Predicate) {
    let [Expr::Field(field), Expr::Literal(value)] = args else {
        unreachable!("collection contains expects field/literal operands")
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        field.as_str().to_string(),
        CompareOp::Contains,
        value.clone(),
        CoercionId::Strict,
    ));

    (when_true.clone(), Predicate::Not(Box::new(when_true)))
}

fn compile_bool_text_target(expr: &Expr) -> (String, CoercionId) {
    match expr {
        Expr::Field(field) => (field.as_str().to_string(), CoercionId::Strict),
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => (field.as_str().to_string(), CoercionId::TextCasefold),
            _ => unreachable!("boolean text targets only compile LOWER(field) wrappers"),
        },
        _ => unreachable!("boolean text targets only compile canonical field wrappers"),
    }
}

fn compile_ready_bool_expr(expr: &Expr) -> bool {
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
            ) && compile_ready_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => compile_ready_bool_expr(left.as_ref()) && compile_ready_bool_expr(right.as_ref()),
        Expr::Binary { op, left, right } => compile_ready_bool_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            compile_ready_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                compile_ready_bool_expr(arm.condition()) && compile_ready_bool_expr(arm.result())
            }) && compile_ready_bool_expr(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

fn compile_ready_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    match op {
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte => match (left, right) {
            (Expr::Field(_), Expr::Literal(_) | Expr::Field(_)) => true,
            (
                Expr::FunctionCall {
                    function: Function::Lower,
                    args,
                },
                Expr::Literal(Value::Text(_)),
            ) => matches!(args.as_slice(), [Expr::Field(_)]),
            _ => false,
        },
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => false,
    }
}

fn compile_ready_bool_function_call(function: Function, args: &[Expr]) -> bool {
    match function {
        Function::IsNull | Function::IsNotNull => {
            matches!(args, [Expr::Field(_) | Expr::Literal(_)])
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            matches!(args, [left, Expr::Literal(Value::Text(_))] if compile_ready_text_target(left))
        }
        Function::IsMissing | Function::IsEmpty | Function::IsNotEmpty => {
            matches!(args, [Expr::Field(_)])
        }
        Function::CollectionContains => matches!(args, [Expr::Field(_), Expr::Literal(_)]),
        _ => false,
    }
}

fn compile_ready_text_target(expr: &Expr) -> bool {
    match expr {
        Expr::Field(_) => true,
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => matches!(args.as_slice(), [Expr::Field(_)]),
        _ => false,
    }
}

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
        | BinaryOp::Div => unreachable!("non-compare BinaryOp cannot lower to CompareOp"),
    }
}

const fn compare_literal_coercion(op: CompareOp, value: &Value) -> CoercionId {
    match value {
        Value::Text(_) | Value::Uint(_) | Value::Uint128(_) | Value::UintBig(_) => {
            CoercionId::Strict
        }
        Value::Float32(_) | Value::Float64(_) | Value::Decimal(_) => match op {
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
                CoercionId::NumericWiden
            }
            CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::In
            | CompareOp::NotIn
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => CoercionId::Strict,
        },
        _ if value.supports_numeric_coercion() => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    }
}

fn compare_field_coercion(op: CompareOp) -> CoercionId {
    match op {
        CompareOp::Eq | CompareOp::Ne => CoercionId::Strict,
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => CoercionId::NumericWiden,
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => {
            unreachable!("non-field compare operator cannot lower to CompareFieldsPredicate")
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{CoercionId, CompareFieldsPredicate, ComparePredicate, Predicate},
        value::{Value, ValueEnum},
    };

    use super::{
        Expr, canonicalize_predicate_via_bool_expr, compile_bool_expr_to_predicate,
        is_normalized_bool_expr, normalize_bool_expr, predicate_to_bool_expr,
    };

    #[test]
    fn predicate_bridge_roundtrip_covers_every_live_predicate_variant() {
        for predicate in representative_predicates() {
            let expr = predicate_to_bool_expr(&predicate);
            assert!(
                expr_has_no_opaque_nodes(&expr),
                "predicate lowered through opaque expr shape: {predicate:?}"
            );

            let normalized = normalize_bool_expr(expr);
            assert!(
                is_normalized_bool_expr(&normalized),
                "predicate did not lower to normalized bool expr: {predicate:?}"
            );

            let round_tripped = compile_bool_expr_to_predicate(&normalized);
            let rerendered = predicate_to_bool_expr(&round_tripped);
            assert!(
                expr_has_no_opaque_nodes(&rerendered),
                "round-tripped predicate reintroduced opaque expr shape: {round_tripped:?}"
            );
        }
    }

    #[test]
    fn predicate_bridge_roundtrip_is_idempotent() {
        for predicate in representative_predicates() {
            let once = canonicalize_predicate_via_bool_expr(predicate.clone());
            let twice = canonicalize_predicate_via_bool_expr(once.clone());

            assert_eq!(
                twice, once,
                "predicate bridge was not idempotent: {predicate:?}"
            );
        }
    }

    #[test]
    fn predicate_bridge_canonicalizes_equivalent_membership_and_logical_shapes() {
        let unsorted_in = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![Value::Uint(3), Value::Uint(1), Value::Uint(3)]),
            CoercionId::Strict,
        ));
        let sorted_in = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![Value::Uint(1), Value::Uint(3)]),
            CoercionId::Strict,
        ));
        let swapped_eq_fields = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "rhs".to_string(),
            crate::db::predicate::CompareOp::Eq,
            "lhs".to_string(),
            CoercionId::Strict,
        ));
        let ordered_eq_fields = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "lhs".to_string(),
            crate::db::predicate::CompareOp::Eq,
            "rhs".to_string(),
            CoercionId::Strict,
        ));
        let nested_and = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
            Predicate::And(vec![Predicate::Compare(ComparePredicate::eq(
                "a".to_string(),
                Value::Int(1),
            ))]),
        ]);
        let flat_and = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq("a".to_string(), Value::Int(1))),
            Predicate::Compare(ComparePredicate::eq("b".to_string(), Value::Int(2))),
        ]);

        assert_eq!(
            canonicalize_predicate_via_bool_expr(unsorted_in),
            canonicalize_predicate_via_bool_expr(sorted_in)
        );
        assert_eq!(
            canonicalize_predicate_via_bool_expr(swapped_eq_fields),
            canonicalize_predicate_via_bool_expr(ordered_eq_fields)
        );
        assert_eq!(
            canonicalize_predicate_via_bool_expr(nested_and),
            canonicalize_predicate_via_bool_expr(flat_and)
        );
    }

    #[test]
    fn predicate_bridge_preserves_special_predicate_variants() {
        let text_contains_ci = Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("al".to_string()),
        };
        let is_missing = Predicate::IsMissing {
            field: "nickname".to_string(),
        };
        let contains = Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            crate::db::predicate::CompareOp::Contains,
            Value::Text("mage".to_string()),
            CoercionId::Strict,
        ));

        assert!(matches!(
            canonicalize_predicate_via_bool_expr(text_contains_ci),
            Predicate::TextContainsCi { .. }
        ));
        assert!(matches!(
            canonicalize_predicate_via_bool_expr(is_missing),
            Predicate::IsMissing { .. }
        ));
        assert!(matches!(
            canonicalize_predicate_via_bool_expr(contains),
            Predicate::Compare(compare)
                if compare.op() == crate::db::predicate::CompareOp::Contains
        ));
    }

    fn representative_predicates() -> Vec<Predicate> {
        vec![
            Predicate::True,
            Predicate::False,
            Predicate::And(vec![
                Predicate::Compare(ComparePredicate::eq("age".to_string(), Value::Int(5))),
                Predicate::Not(Box::new(Predicate::IsNull {
                    field: "name".to_string(),
                })),
            ]),
            Predicate::Or(vec![
                Predicate::TextContains {
                    field: "name".to_string(),
                    value: Value::Text("al".to_string()),
                },
                Predicate::IsEmpty {
                    field: "tags".to_string(),
                },
            ]),
            Predicate::Compare(ComparePredicate::with_coercion(
                "stage",
                crate::db::predicate::CompareOp::Eq,
                Value::Enum(ValueEnum::loose("Active")),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                crate::db::predicate::CompareOp::Lt,
                Value::Int(10),
                CoercionId::NumericWiden,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                crate::db::predicate::CompareOp::In,
                Value::List(vec![Value::Uint(3), Value::Uint(1), Value::Uint(3)]),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                crate::db::predicate::CompareOp::NotIn,
                Value::List(vec![Value::Uint(7), Value::Uint(2)]),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "tags",
                crate::db::predicate::CompareOp::Contains,
                Value::Text("mage".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                crate::db::predicate::CompareOp::StartsWith,
                Value::Text("Al".to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                crate::db::predicate::CompareOp::EndsWith,
                Value::Text("ce".to_string()),
                CoercionId::Strict,
            )),
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "rhs".to_string(),
                crate::db::predicate::CompareOp::Eq,
                "lhs".to_string(),
                CoercionId::Strict,
            )),
            Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
                "level".to_string(),
                crate::db::predicate::CompareOp::Gt,
                "rank".to_string(),
                CoercionId::NumericWiden,
            )),
            Predicate::IsNull {
                field: "deleted_at".to_string(),
            },
            Predicate::IsNotNull {
                field: "name".to_string(),
            },
            Predicate::IsMissing {
                field: "nickname".to_string(),
            },
            Predicate::IsEmpty {
                field: "tags".to_string(),
            },
            Predicate::IsNotEmpty {
                field: "tags".to_string(),
            },
            Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Text("li".to_string()),
            },
            Predicate::TextContainsCi {
                field: "name".to_string(),
                value: Value::Text("al".to_string()),
            },
        ]
    }

    fn expr_has_no_opaque_nodes(expr: &Expr) -> bool {
        match expr {
            Expr::Field(_) | Expr::Literal(_) => true,
            Expr::Unary { expr, .. } => expr_has_no_opaque_nodes(expr),
            Expr::Binary { left, right, .. } => {
                expr_has_no_opaque_nodes(left) && expr_has_no_opaque_nodes(right)
            }
            Expr::FunctionCall { args, .. } => args.iter().all(expr_has_no_opaque_nodes),
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    expr_has_no_opaque_nodes(arm.condition())
                        && expr_has_no_opaque_nodes(arm.result())
                }) && expr_has_no_opaque_nodes(else_expr)
            }
            Expr::Aggregate(_) => false,
            #[cfg(test)]
            Expr::Alias { .. } => false,
        }
    }

    #[test]
    fn predicate_bridge_preserves_strict_ordered_text_compares() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "name".to_string(),
            crate::db::predicate::CompareOp::Gte,
            Value::Text("Ada".to_string()),
            CoercionId::Strict,
        ));

        assert_eq!(
            canonicalize_predicate_via_bool_expr(predicate.clone()),
            predicate
        );
    }

    #[test]
    fn predicate_bridge_preserves_strict_uint_ordered_compares() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "rank".to_string(),
            crate::db::predicate::CompareOp::Gt,
            Value::Uint(10),
            CoercionId::Strict,
        ));

        assert_eq!(
            canonicalize_predicate_via_bool_expr(predicate.clone()),
            predicate
        );
    }

    #[test]
    fn predicate_bridge_promotes_ordered_decimal_literal_compares_to_numeric_widen() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "dodge_chance".to_string(),
            crate::db::predicate::CompareOp::Gte,
            Value::Decimal(crate::types::Decimal::new(20, 2)),
            CoercionId::Strict,
        ));

        assert_eq!(
            canonicalize_predicate_via_bool_expr(predicate),
            Predicate::Compare(ComparePredicate::with_coercion(
                "dodge_chance".to_string(),
                crate::db::predicate::CompareOp::Gte,
                Value::Decimal(crate::types::Decimal::new(20, 2)),
                CoercionId::NumericWiden,
            )),
            "ordered decimal literal compares should canonicalize onto numeric widening so float-backed fields do not fail strict literal validation",
        );
    }
}
