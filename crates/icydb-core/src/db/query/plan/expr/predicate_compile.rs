use crate::{
    db::{
        access::canonical::canonicalize_value_set,
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{
            BinaryOp, CaseWhenArm, Expr, FieldId, Function, UnaryOp,
            truth_condition_binary_compare_op, truth_condition_compare_binary_op,
        },
    },
    value::Value,
};

/// Canonicalize one runtime predicate by routing it through the planner-owned
/// boolean-expression seam and rebuilding the runtime predicate shell from the
/// normalized planner form.
#[must_use]
pub(in crate::db) fn canonicalize_runtime_predicate_via_bool_expr(
    predicate: Predicate,
) -> Predicate {
    let expr = predicate_to_bool_expr(&predicate);
    let expr = super::normalize_bool_expr(expr);

    debug_assert!(super::is_normalized_bool_expr(&expr));

    crate::db::predicate::normalize(&compile_normalized_bool_expr_to_predicate(&expr))
}

/// Compile one normalized planner-owned boolean expression into the canonical
/// runtime predicate tree.
#[must_use]
pub(in crate::db) fn compile_normalized_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    debug_assert!(
        compile_ready_normalized_bool_expr(expr),
        "normalized boolean expression"
    );

    if let Some(predicate) = collapse_membership_bool_expr(expr) {
        return crate::db::predicate::normalize(&predicate);
    }

    crate::db::predicate::normalize(&compile_bool_truth_sets(expr).0)
}

/// Derive the strongest predicate subset supported by the runtime predicate
/// compiler for one normalized planner-owned boolean expression.
#[must_use]
pub(in crate::db) fn derive_normalized_bool_expr_predicate_subset(
    expr: &Expr,
) -> Option<Predicate> {
    compile_ready_normalized_bool_expr(expr)
        .then(|| compile_normalized_bool_expr_to_predicate(expr))
}

/// Test-only export for the runtime-predicate -> planner-expression bridge.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn predicate_to_runtime_bool_expr_for_test(predicate: &Predicate) -> Expr {
    predicate_to_bool_expr(predicate)
}

// Convert one runtime predicate tree into one planner-owned boolean
// expression tree so planner normalization can remain the only semantic branch
// owner before recompiling the runtime predicate shell.
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

// Build one canonical boolean chain from runtime predicate children while
// preserving empty-chain identities.
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

// Convert one runtime compare predicate into the planner-owned boolean
// expression shape consumed by shared normalization and recompilation.
fn compare_predicate_to_bool_expr(compare: &ComparePredicate) -> Expr {
    match compare.op() {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => Expr::Binary {
            op: truth_condition_compare_binary_op(compare.op())
                .expect("binary compare predicates must map onto planner binary operators"),
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

// Convert one field-to-field compare predicate into the planner-owned boolean
// expression shape consumed by shared normalization and recompilation.
fn compare_fields_predicate_to_bool_expr(compare: &CompareFieldsPredicate) -> Expr {
    Expr::Binary {
        op: truth_condition_compare_binary_op(compare.op())
            .expect("field compare predicates must map onto planner binary operators"),
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

// Convert one runtime membership compare back onto the planner OR-of-EQ /
// AND-of-NE spine consumed by shared membership collapse.
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
fn field_function_expr(function: Function, field: &str) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![Expr::Field(FieldId::new(field.to_owned()))],
    }
}

// Build one text-targeted boolean function shell.
fn text_function_expr(function: Function, left: Expr, value: Value) -> Expr {
    Expr::FunctionCall {
        function,
        args: vec![left, Expr::Literal(value)],
    }
}

// Wrap one field in LOWER(...) only for casefold coercion.
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

// Collapse one normalized OR-of-EQ / AND-of-NE membership chain back onto the
// compact runtime `IN` / `NOT IN` predicate form before general truth-set
// compilation re-expands it.
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

// Collect one same-field compare chain and rebuild the canonical runtime
// membership predicate when every leaf targets the same field/coercion pair.
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

// Flatten one associative compare chain so membership collapse can inspect
// every EQ/NE leaf without reopening semantic branching elsewhere.
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

// Extract one membership-safe compare leaf that can round-trip back onto the
// compact runtime `IN` / `NOT IN` predicate surface.
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
            compare_literal_coercion(
                truth_condition_binary_compare_op(*op)
                    .expect("compile-ready compare operands must resolve onto CompareOp"),
                value,
            ),
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

// Compile one normalized boolean expression into the predicate pair that holds
// when the expression is true versus false.
fn compile_bool_truth_sets(expr: &Expr) -> (Predicate, Predicate) {
    debug_assert!(compile_ready_normalized_bool_expr(expr));

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

// Compile one normalized searched-CASE tree by recursively composing the
// truth-set pairs of every branch condition and result arm.
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

// Compile one bare boolean field onto the runtime `field = TRUE/FALSE` pair
// used by the legacy predicate shell.
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

// Compile one normalized compare node onto the runtime true/false predicate
// pair, preserving SQL null behavior by returning the empty truth set for null
// compares.
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

// Compile one compare-ready boolean expression leaf onto the corresponding
// runtime compare predicate.
fn compile_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> Predicate {
    let op = truth_condition_binary_compare_op(op)
        .expect("compile-ready binary compare operators must lower onto CompareOp");

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

// Compile one admitted boolean function onto the runtime true/false predicate
// pair that preserves the same planner-owned boolean shape.
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

// Compile one null-test function onto the corresponding runtime null predicate
// pair while preserving literal-null constant behavior.
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

// Compile one STARTS_WITH / ENDS_WITH boolean function onto the runtime prefix
// predicate pair over the canonical text target wrapper.
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

// Compile one CONTAINS text predicate onto the runtime text predicate shell
// while preserving strict versus casefold coercion.
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

// Compile one single-field boolean function onto its runtime predicate pair.
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

// Compile one collection-membership boolean function onto the runtime compare
// predicate shell.
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

// Project one canonical text target wrapper onto the runtime field/coercion
// pair consumed by text predicate shells.
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

// Admit only the normalized boolean-expression shapes that the runtime
// predicate shell can represent without reopening semantic branching.
fn compile_ready_normalized_bool_expr(expr: &Expr) -> bool {
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
            ) && compile_ready_normalized_bool_expr(expr.as_ref())
        }
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => {
            compile_ready_normalized_bool_expr(left.as_ref())
                && compile_ready_normalized_bool_expr(right.as_ref())
        }
        Expr::Binary { op, left, right } => compile_ready_bool_compare_expr(*op, left, right),
        Expr::FunctionCall { function, args } => {
            compile_ready_bool_function_call(*function, args.as_slice())
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                compile_ready_normalized_bool_expr(arm.condition())
                    && compile_ready_normalized_bool_expr(arm.result())
            }) && compile_ready_normalized_bool_expr(else_expr.as_ref())
        }
        Expr::Aggregate(_) | Expr::Literal(_) => false,
        #[cfg(test)]
        Expr::Alias { .. } => false,
    }
}

// Admit only the normalized compare shapes that can lower directly onto the
// runtime predicate compare shells.
fn compile_ready_bool_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
    if truth_condition_binary_compare_op(op).is_none() {
        return false;
    }

    match (left, right) {
        (Expr::Field(_), Expr::Literal(_) | Expr::Field(_)) => true,
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            },
            Expr::Literal(Value::Text(_)),
        ) => matches!(args.as_slice(), [Expr::Field(_)]),
        _ => false,
    }
}

// Admit only the normalized boolean function calls that have a direct runtime
// predicate shell.
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

// Admit only canonical text targets that map directly onto the runtime text
// predicate shells.
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
