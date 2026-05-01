//! Module: query::plan::expr::predicate_compile
//! Responsibility: compile already-normalized planner boolean expressions into
//! runtime predicate shells and predicate subsets.
//! Does not own: schema type inference, boolean canonicalization, projection
//! evaluation, or scalar expression execution.
//! Boundary: consumes runtime-admissible canonical boolean shape and may
//! select leaf-local runtime predicate coercions while lowering
//! already-canonical compare/function leaves, but it must not rediscover
//! expression types or rewrite expression shape.

use crate::{
    db::{
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, MembershipCompareLeaf,
            Predicate, collapse_membership_compare_leaves,
        },
        query::plan::expr::{
            BinaryOp, BooleanFunctionShape, CanonicalExpr, CaseWhenArm, Expr,
            FieldPredicateFunctionKind, Function, NullTestFunctionKind, TextPredicateFunctionKind,
            UnaryOp, truth_condition_binary_compare_op,
        },
    },
    value::Value,
};

///
/// PredicateCompilation
///
/// Stage artifact for one runtime predicate produced by the predicate
/// compilation boundary. It makes the compile boundary explicit while planner
/// and executor boundaries continue to exchange the underlying `Predicate`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PredicateCompilation {
    predicate: Predicate,
}

impl PredicateCompilation {
    // Build one predicate compilation artifact after this module has completed
    // all predicate-shape lowering.
    const fn new(predicate: Predicate) -> Self {
        Self { predicate }
    }

    /// Return the runtime predicate for existing execution and lowering
    /// surfaces.
    pub(in crate::db) fn into_predicate(self) -> Predicate {
        self.predicate
    }
}

/// Compile one canonical planner-owned boolean expression artifact into an
/// explicit predicate stage artifact.
#[must_use]
pub(in crate::db) fn compile_canonical_bool_expr_to_compiled_predicate(
    expr: &CanonicalExpr,
) -> PredicateCompilation {
    PredicateCompilation::new(compile_normalized_bool_expr_to_predicate_impl(
        expr.as_expr(),
    ))
}

/// Compile one normalized planner-owned boolean expression into the canonical
/// runtime predicate tree after validating it can be represented by the
/// canonical expression artifact.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compile_normalized_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    let canonical = CanonicalExpr::from_normalized_bool_expr(expr)
        .expect("predicate compilation requires normalized boolean expression canonical shape");

    compile_canonical_bool_expr_to_compiled_predicate(&canonical).into_predicate()
}

fn compile_normalized_bool_expr_to_predicate_impl(expr: &Expr) -> Predicate {
    debug_assert!(
        runtime_predicate_admissible_expr(expr),
        "normalized boolean expression"
    );

    if let Some(predicate) = collapse_membership_bool_expr(expr) {
        return crate::db::predicate::normalize(&predicate);
    }

    crate::db::predicate::normalize(&compile_bool_truth_sets(expr).0)
}

/// Derive the strongest predicate subset supported by the runtime predicate
/// compiler for one canonical planner-owned boolean expression artifact.
#[must_use]
pub(in crate::db) fn derive_canonical_bool_expr_predicate_subset(
    expr: &CanonicalExpr,
) -> Option<Predicate> {
    runtime_predicate_admissible_expr(expr.as_expr())
        .then(|| compile_canonical_bool_expr_to_compiled_predicate(expr).into_predicate())
}

/// Derive the strongest predicate subset supported by the runtime predicate
/// compiler for one normalized boolean expression after validating that it can
/// cross the canonical expression artifact boundary.
#[must_use]
pub(in crate::db) fn derive_normalized_bool_expr_predicate_subset(
    expr: &Expr,
) -> Option<Predicate> {
    let canonical = CanonicalExpr::from_normalized_bool_expr(expr)?;

    derive_canonical_bool_expr_predicate_subset(&canonical)
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
        | Expr::FieldPath(_)
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

    let mut membership_leaves = Vec::with_capacity(leaves.len());

    for leaf in leaves {
        let (leaf_field, leaf_value, leaf_coercion) = membership_compare_leaf(leaf, compare_op)?;
        membership_leaves.push((leaf_field, leaf_value, leaf_coercion));
    }

    let (_, _, first_coercion) = membership_leaves.first()?;
    // Fail closed before handing leaves to the shared membership assembler.
    // Mixed strict/casefold leaves are semantically different even when they
    // target the same field and must remain as expanded OR/AND chains.
    if !membership_leaves
        .iter()
        .all(|(_, _, coercion)| coercion == first_coercion)
    {
        return None;
    }

    collapse_membership_compare_leaves(
        membership_leaves
            .into_iter()
            .map(|(field, value, coercion)| MembershipCompareLeaf::new(field, value, coercion)),
        target_op,
    )
    .map(Predicate::Compare)
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
        | Expr::FieldPath(_)
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
// when the expression is TRUE versus FALSE under the shared `truth_value`
// contract. SQL UNKNOWN is represented by neither set; the exported compiled
// predicate uses only the TRUE set, so runtime row filtering admits exactly the
// same rows as evaluating the expression and applying TRUE-only admission.
fn compile_bool_truth_sets(expr: &Expr) -> (Predicate, Predicate) {
    debug_assert!(runtime_predicate_admissible_expr(expr));

    match expr {
        Expr::Field(field) => compile_bool_field_truth_sets(field.as_str()),
        Expr::Literal(Value::Bool(true)) => (Predicate::True, Predicate::False),
        Expr::Literal(Value::Bool(false)) => (Predicate::False, Predicate::True),
        Expr::Literal(Value::Null) => (Predicate::False, Predicate::False),
        Expr::Literal(_) => {
            unreachable!("boolean compilation expects only boolean-context literals")
        }
        Expr::FieldPath(_) => {
            unreachable!("boolean compilation expects compile-ready field leaves")
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
// used by the predicate shell.
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
// pair, preserving three-valued null behavior by returning the empty truth set
// for null compares.
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
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::NullTest) => compile_bool_null_test_function_truth_sets(
            function
                .boolean_null_test_kind()
                .expect("null-test boolean family must keep one null-test kind"),
            args,
        ),
        Some(BooleanFunctionShape::TextPredicate) => {
            let kind = boolean_text_predicate_kind(function);

            match kind {
                TextPredicateFunctionKind::StartsWith | TextPredicateFunctionKind::EndsWith => {
                    compile_bool_prefix_text_function_truth_sets(kind, args)
                }
                TextPredicateFunctionKind::Contains => {
                    compile_bool_contains_function_truth_sets(args)
                }
            }
        }
        Some(BooleanFunctionShape::FieldPredicate) => match function
            .boolean_field_predicate_kind()
            .expect("field-predicate boolean family must keep one field-predicate kind")
        {
            FieldPredicateFunctionKind::Missing => {
                compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsMissing {
                    field: field.to_string(),
                })
            }
            FieldPredicateFunctionKind::Empty => {
                compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsEmpty {
                    field: field.to_string(),
                })
            }
            FieldPredicateFunctionKind::NotEmpty => {
                compile_bool_field_predicate_truth_sets(args, |field| Predicate::IsNotEmpty {
                    field: field.to_string(),
                })
            }
        },
        Some(BooleanFunctionShape::CollectionContains) => {
            compile_bool_collection_contains_truth_sets(args)
        }
        Some(BooleanFunctionShape::TruthCoalesce) | None => {
            unreachable!("boolean compilation expects only directly compilable boolean functions")
        }
    }
}

// Resolve the finer text-predicate kind after the caller has already matched
// the broad boolean text-predicate function shape.
const fn boolean_text_predicate_kind(function: Function) -> TextPredicateFunctionKind {
    function
        .boolean_text_predicate_kind()
        .expect("text-predicate boolean family must keep one text-predicate kind")
}

// Compile one null-test function onto the corresponding runtime null predicate
// pair while preserving literal-null constant behavior.
fn compile_bool_null_test_function_truth_sets(
    kind: NullTestFunctionKind,
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

            if kind.null_matches_true() {
                (is_null, is_not_null)
            } else {
                (is_not_null, is_null)
            }
        }
        Expr::Literal(value) => {
            let literal_is_true = kind.null_matches_true() == matches!(value, Value::Null);

            if literal_is_true {
                (Predicate::True, Predicate::False)
            } else {
                (Predicate::False, Predicate::True)
            }
        }
        _ => unreachable!("boolean null tests expect field/literal operands"),
    }
}

// Compile one STARTS_WITH / ENDS_WITH boolean function onto the runtime prefix
// predicate pair over the canonical text target wrapper.
fn compile_bool_prefix_text_function_truth_sets(
    kind: TextPredicateFunctionKind,
    args: &[Expr],
) -> (Predicate, Predicate) {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        unreachable!("boolean prefix text predicates keep field/text operands")
    };
    let (field, coercion) = compile_bool_text_target(left);
    let op = match kind {
        TextPredicateFunctionKind::StartsWith => CompareOp::StartsWith,
        TextPredicateFunctionKind::EndsWith => CompareOp::EndsWith,
        TextPredicateFunctionKind::Contains => {
            unreachable!("prefix compiler called with non-prefix text-predicate kind")
        }
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

/// Subset of `TruthAdmission` that the runtime predicate engine can represent.
/// Must remain a strict subset of canonicalized boolean expressions.
fn runtime_predicate_admissible_expr(expr: &Expr) -> bool {
    RuntimePredicateAdmission::is_admissible(expr)
}

///
/// RuntimePredicateAdmission
///
/// Runtime predicate admission owns the capability boundary between the
/// planner's canonical boolean IR and the smaller runtime predicate AST. It is
/// used only by predicate compilation to decide whether a canonical expression
/// can lower without inventing new planner rewrites or silently changing SQL
/// three-valued boolean semantics.
///

struct RuntimePredicateAdmission;

impl RuntimePredicateAdmission {
    // Admit only normalized boolean-expression shapes that the runtime
    // predicate shell can represent without reopening semantic branching.
    fn is_admissible(expr: &Expr) -> bool {
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
                ) && Self::is_admissible(expr.as_ref())
            }
            Expr::Binary {
                op: BinaryOp::And | BinaryOp::Or,
                left,
                right,
            } => Self::is_admissible(left.as_ref()) && Self::is_admissible(right.as_ref()),
            Expr::Binary { op, left, right } => Self::is_compare_expr(*op, left, right),
            Expr::FunctionCall { function, args } => {
                Self::is_bool_function_call(*function, args.as_slice())
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    Self::is_admissible(arm.condition()) && Self::is_admissible(arm.result())
                }) && Self::is_admissible(else_expr.as_ref())
            }
            Expr::FieldPath(_) | Expr::Aggregate(_) | Expr::Literal(_) => false,
            #[cfg(test)]
            Expr::Alias { .. } => false,
        }
    }

    // Admit only normalized compare shapes that lower directly onto runtime
    // predicate compare shells.
    fn is_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
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

    // Admit only normalized boolean function calls that have a direct runtime
    // predicate shell.
    fn is_bool_function_call(function: Function, args: &[Expr]) -> bool {
        match function.boolean_function_shape() {
            Some(BooleanFunctionShape::NullTest) => {
                matches!(args, [Expr::Field(_) | Expr::Literal(_)])
            }
            Some(BooleanFunctionShape::TextPredicate) => {
                matches!(args, [left, Expr::Literal(Value::Text(_))] if Self::is_text_target(left))
            }
            Some(BooleanFunctionShape::FieldPredicate) => {
                matches!(args, [Expr::Field(_)])
            }
            Some(BooleanFunctionShape::CollectionContains) => {
                matches!(args, [Expr::Field(_), Expr::Literal(_)])
            }
            Some(BooleanFunctionShape::TruthCoalesce) | None => false,
        }
    }

    // Admit only canonical text targets that map directly onto runtime text
    // predicate shells.
    fn is_text_target(expr: &Expr) -> bool {
        match expr {
            Expr::Field(_) => true,
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            } => matches!(args.as_slice(), [Expr::Field(_)]),
            _ => false,
        }
    }
}

const fn compare_literal_coercion(op: CompareOp, value: &Value) -> CoercionId {
    match value {
        Value::Text(_) | Value::Uint(_) | Value::Uint128(_) | Value::UintBig(_) => {
            CoercionId::Strict
        }
        Value::Float32(_) | Value::Float64(_) | Value::Decimal(_) => {
            if op.is_ordering_family() {
                CoercionId::NumericWiden
            } else {
                CoercionId::Strict
            }
        }
        _ if value.supports_numeric_coercion() => CoercionId::NumericWiden,
        _ => CoercionId::Strict,
    }
}

fn compare_field_coercion(op: CompareOp) -> CoercionId {
    if !op.supports_field_compare() {
        unreachable!("non-field compare operator cannot lower to CompareFieldsPredicate");
    }

    if op.is_ordering_family() {
        CoercionId::NumericWiden
    } else {
        CoercionId::Strict
    }
}
