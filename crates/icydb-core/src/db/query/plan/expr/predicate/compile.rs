//! Module: query::plan::expr::predicate::compile
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
        QueryError,
        predicate::{
            CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, MembershipCompareLeaf,
            Predicate, canonical_membership_value_list, collapse_membership_compare_leaves,
        },
        query::plan::expr::{
            BinaryOp, BooleanFunctionShape, Expr, FieldPredicateFunctionKind, Function,
            NullTestFunctionKind, TextPredicateFunctionKind, UnaryOp, is_normalized_bool_expr,
            truth_condition_binary_compare_op,
        },
        query::preparation::PreparationWork,
    },
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

// Unsupported shape is a capability result; resource exhaustion must escape
// speculative membership attempts and cannot become an absent predicate.
#[derive(Debug)]
enum PredicateCompileError {
    Unsupported,
    Resource(QueryError),
}

type CompileResult<T> = Result<T, PredicateCompileError>;

impl From<QueryError> for PredicateCompileError {
    fn from(error: QueryError) -> Self {
        Self::Resource(error)
    }
}

fn optional_predicate(result: CompileResult<Predicate>) -> Result<Option<Predicate>, QueryError> {
    match result {
        Ok(predicate) => Ok(Some(predicate)),
        Err(PredicateCompileError::Unsupported) => Ok(None),
        Err(PredicateCompileError::Resource(error)) => Err(error),
    }
}

/// Compile a maintained normalized expression in a finite fixture request.
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) fn compile_normalized_bool_expr_to_predicate(expr: &Expr) -> Predicate {
    crate::db::query::preparation::with_preparation_work(|work| {
        derive_normalized_bool_expr_predicate_subset(expr, work)
            .expect("fixture construction fits")
            .expect("predicate compilation requires a normalized admissible expression")
    })
}

fn compile_normalized_bool_expr_to_predicate_impl(
    expr: &Expr,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    if !runtime_predicate_admissible_expr(expr) {
        return Err(PredicateCompileError::Unsupported);
    }
    let when_true = compile_bool_truth_predicate(expr, BoolTruth::True, work)?;
    Ok(crate::db::predicate::normalize(when_true))
}

/// Derive a runtime predicate without copying the canonical expression tree.
/// Unsupported shapes return absence; construction exhaustion remains an error.
pub(in crate::db) fn derive_normalized_bool_expr_predicate_subset(
    expr: &Expr,
    work: &PreparationWork<'_>,
) -> Result<Option<Predicate>, QueryError> {
    if !is_normalized_bool_expr(expr) {
        return Ok(None);
    }
    optional_predicate(compile_normalized_bool_expr_to_predicate_impl(expr, work))
}

// Collapse one normalized OR-of-EQ / AND-of-NE membership chain back onto the
// compact runtime `IN` / `NOT IN` predicate form before general truth-set
// compilation re-expands it.
fn collapse_membership_bool_expr(
    expr: &Expr,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    match expr {
        Expr::Binary {
            op: BinaryOp::Or, ..
        } => {
            collapse_same_field_compare_chain(expr, BinaryOp::Or, BinaryOp::Eq, CompareOp::In, work)
        }
        Expr::Binary {
            op: BinaryOp::And, ..
        } => collapse_same_field_compare_chain(
            expr,
            BinaryOp::And,
            BinaryOp::Ne,
            CompareOp::NotIn,
            work,
        ),
        Expr::Field(_)
        | Expr::FieldPath(_)
        | Expr::Literal(_)
        | Expr::Unary { .. }
        | Expr::Aggregate(_)
        | Expr::FunctionCall { .. }
        | Expr::Case { .. }
        | Expr::Binary { .. } => Err(PredicateCompileError::Unsupported),
        #[cfg(test)]
        Expr::Alias { .. } => Err(PredicateCompileError::Unsupported),
    }
}

// Collect one same-field compare chain and rebuild the canonical runtime
// membership predicate when every leaf targets the same field/coercion pair.
fn collapse_same_field_compare_chain(
    expr: &Expr,
    join_op: BinaryOp,
    compare_op: BinaryOp,
    target_op: CompareOp,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let mut leaves = Vec::new();
    collect_compare_chain(expr, join_op, &mut leaves, work)?;

    let mut membership_leaves = work.vec_with_capacity(leaves.len())?;
    for leaf in leaves {
        let (leaf_field, leaf_value, leaf_coercion) =
            membership_compare_leaf(leaf, compare_op).ok_or(PredicateCompileError::Unsupported)?;
        membership_leaves.push(MembershipCompareLeaf::new(
            leaf_field,
            work.copy_value(leaf_value)?,
            leaf_coercion,
        ));
    }

    collapse_membership_compare_leaves(membership_leaves, target_op, work)
        .map_err(QueryError::execute)?
        .map(Predicate::Compare)
        .ok_or(PredicateCompileError::Unsupported)
}

// Flatten one associative compare chain so membership collapse can inspect
// every EQ/NE leaf without reopening semantic branching elsewhere.
fn collect_compare_chain<'a>(
    expr: &'a Expr,
    join_op: BinaryOp,
    out: &mut Vec<&'a Expr>,
    work: &PreparationWork<'_>,
) -> CompileResult<()> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match expr {
        Expr::Binary { op, left, right } if *op == join_op => {
            collect_compare_chain(left.as_ref(), join_op, out, work)?;
            collect_compare_chain(right.as_ref(), join_op, out, work)
        }
        Expr::Binary { .. } => {
            work.reserve_vec(out, 1)?;
            out.push(expr);
            Ok(())
        }
        Expr::Field(_)
        | Expr::FieldPath(_)
        | Expr::Literal(_)
        | Expr::Unary { .. }
        | Expr::Aggregate(_)
        | Expr::FunctionCall { .. }
        | Expr::Case { .. } => Err(PredicateCompileError::Unsupported),
        #[cfg(test)]
        Expr::Alias { .. } => Err(PredicateCompileError::Unsupported),
    }
}

// Extract one membership-safe compare leaf that can round-trip back onto the
// compact runtime `IN` / `NOT IN` predicate surface.
fn membership_compare_leaf(
    expr: &Expr,
    compare_op: BinaryOp,
) -> Option<(&str, &Value, CoercionId)> {
    let Expr::Binary { op, left, right } = expr else {
        return None;
    };
    if *op != compare_op {
        return None;
    }
    let compare_op = truth_condition_binary_compare_op(*op)?;

    match (left.as_ref(), right.as_ref()) {
        (Expr::Field(field), Expr::Literal(value)) if membership_value_is_in_safe(value) => Some((
            field.as_str(),
            value,
            compare_literal_coercion(compare_op, value),
        )),
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            },
            Expr::Literal(value @ Value::Text(_)),
        ) => match args.as_slice() {
            [Expr::Field(field)] => Some((field.as_str(), value, CoercionId::TextCasefold)),
            _ => None,
        },
        _ => None,
    }
}

const fn membership_value_is_in_safe(value: &Value) -> bool {
    !matches!(value, Value::Null | Value::List(_) | Value::Map(_))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BoolTruth {
    True,
    False,
}

impl BoolTruth {
    const fn invert(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
        }
    }

    const fn matches_bool(self, value: bool) -> bool {
        matches!((self, value), (Self::True, true) | (Self::False, false))
    }
}

fn wrap_truth_predicate(
    when_true: Predicate,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    if matches!(truth, BoolTruth::True) {
        Ok(when_true)
    } else {
        work.charge(Resource::TemporaryBytes, size_of::<Predicate>() as u64)?;
        Ok(Predicate::Not(Box::new(when_true)))
    }
}

// Compile one normalized boolean expression into the predicate that holds when
// the expression is TRUE or FALSE under the shared `truth_value` contract. SQL
// UNKNOWN is represented by neither set. The access predicate compiler normally
// asks only for TRUE, while NOT requests FALSE from its child.
fn compile_bool_truth_predicate(
    expr: &Expr,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    if let Some(predicate) = optional_predicate(collapse_membership_bool_expr(expr, work))? {
        return compile_membership_truth_predicate(predicate, truth);
    }

    Ok(match expr {
        Expr::Field(field) => compile_bool_field_truth_predicate(field.as_str(), truth, work)?,
        Expr::Literal(Value::Bool(value)) => {
            if truth.matches_bool(*value) {
                Predicate::True
            } else {
                Predicate::False
            }
        }
        Expr::Literal(Value::Null) => Predicate::False,
        Expr::Literal(_) | Expr::FieldPath(_) => return Err(PredicateCompileError::Unsupported),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => return compile_bool_truth_predicate(expr.as_ref(), truth.invert(), work),
        Expr::Binary {
            op: op @ (BinaryOp::And | BinaryOp::Or),
            left,
            right,
        } => {
            let mut children = work.vec_with_capacity(2)?;
            children.push(compile_bool_truth_predicate(left, truth, work)?);
            children.push(compile_bool_truth_predicate(right, truth, work)?);
            // FALSE swaps AND/OR while requesting each child's FALSE truth set.
            if matches!(
                (*op, truth),
                (BinaryOp::And, BoolTruth::True) | (BinaryOp::Or, BoolTruth::False)
            ) {
                Predicate::And(children)
            } else {
                Predicate::Or(children)
            }
        }
        Expr::Binary { op, left, right } => {
            return compile_bool_compare_truth_predicate(
                *op,
                left.as_ref(),
                right.as_ref(),
                truth,
                work,
            );
        }
        Expr::FunctionCall { function, args } => {
            return compile_bool_function_truth_predicate(*function, args, truth, work);
        }
        Expr::Case { .. } | Expr::Aggregate(_) => return Err(PredicateCompileError::Unsupported),
        #[cfg(test)]
        Expr::Alias { .. } => return Err(PredicateCompileError::Unsupported),
    })
}

// Compile a recovered compact membership expression without routing it through
// recursive predicate normalization, which can partially collapse wide OR
// chains before the full same-field set is visible.
fn compile_membership_truth_predicate(
    predicate: Predicate,
    truth: BoolTruth,
) -> CompileResult<Predicate> {
    let Predicate::Compare(mut compare) = predicate else {
        return Err(PredicateCompileError::Unsupported);
    };
    if matches!(truth, BoolTruth::False) {
        compare.op = match compare.op() {
            CompareOp::In => CompareOp::NotIn,
            CompareOp::NotIn => CompareOp::In,
            _ => return Err(PredicateCompileError::Unsupported),
        };
    }
    Ok(Predicate::Compare(compare))
}

// Compile one bare boolean field onto the requested runtime `field = bool`
// predicate shell.
fn compile_bool_field_truth_predicate(
    field: &str,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        work.copy_text(field)?,
        CompareOp::Eq,
        Value::Bool(matches!(truth, BoolTruth::True)),
        CoercionId::Strict,
    )))
}

// Compile one normalized compare node onto the requested runtime truth branch,
// preserving three-valued null behavior by returning the empty truth set for
// null compares.
fn compile_bool_compare_truth_predicate(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    if matches!(left, Expr::Literal(Value::Null)) || matches!(right, Expr::Literal(Value::Null)) {
        return Ok(Predicate::False);
    }

    let when_true = compile_bool_compare_leaf(op, left, right, work)?;
    wrap_truth_predicate(when_true, truth, work)
}

// Compile one compare-ready boolean expression leaf onto the corresponding
// runtime compare predicate.
#[cfg(feature = "sql")]
pub(in crate::db) fn compile_bool_compare_expr(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    work: &PreparationWork<'_>,
) -> Result<Option<Predicate>, QueryError> {
    optional_predicate(compile_bool_compare_leaf(op, left, right, work))
}

fn compile_bool_compare_leaf(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let op = truth_condition_binary_compare_op(op).ok_or(PredicateCompileError::Unsupported)?;

    match (left, right) {
        (field @ (Expr::Field(_) | Expr::FieldPath(_)), Expr::Literal(value)) => {
            Ok(Predicate::Compare(ComparePredicate::with_coercion(
                predicate_field_label(field, work)?,
                op,
                work.copy_value(value)?,
                compare_literal_coercion(op, value),
            )))
        }
        (Expr::Literal(value), field @ (Expr::Field(_) | Expr::FieldPath(_))) => {
            Ok(Predicate::Compare(ComparePredicate::with_coercion(
                predicate_field_label(field, work)?,
                op.flipped(),
                work.copy_value(value)?,
                compare_literal_coercion(op.flipped(), value),
            )))
        }
        (Expr::Field(left_field), Expr::Field(right_field)) => Ok(Predicate::CompareFields(
            CompareFieldsPredicate::with_coercion(
                work.copy_text(left_field.as_str())?,
                op,
                work.copy_text(right_field.as_str())?,
                compare_field_coercion(op).ok_or(PredicateCompileError::Unsupported)?,
            ),
        )),
        (
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            },
            Expr::Literal(Value::Text(value)),
        ) => match args.as_slice() {
            [Expr::Field(field)] => Ok(Predicate::Compare(ComparePredicate::with_coercion(
                work.copy_text(field.as_str())?,
                op,
                Value::Text(work.copy_text(value)?),
                CoercionId::TextCasefold,
            ))),
            _ => Err(PredicateCompileError::Unsupported),
        },
        _ => Err(PredicateCompileError::Unsupported),
    }
}

// Preserve one canonical dotted identity for planner-only nested compare
// predicates. Runtime keeps the expression lane when access cannot discharge
// this predicate, so this label is never mistaken for a top-level row field.
fn predicate_field_label(expr: &Expr, work: &PreparationWork<'_>) -> CompileResult<String> {
    match expr {
        Expr::Field(field) => Ok(work.copy_text(field.as_str())?),
        Expr::FieldPath(path) => Ok(work.render_text(|output| {
            output.write_str(path.root().as_str())?;
            for segment in path.segments() {
                output.write_str(".")?;
                output.write_str(segment)?;
            }
            Ok(())
        })?),
        _ => Err(PredicateCompileError::Unsupported),
    }
}

// Compile one admitted boolean function onto the requested runtime truth
// branch while preserving the same planner-owned boolean shape.
fn compile_bool_function_truth_predicate(
    function: Function,
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::NullTest) => compile_bool_null_test_function_truth_predicate(
            function
                .boolean_null_test_kind()
                .ok_or(PredicateCompileError::Unsupported)?,
            args,
            truth,
            work,
        ),
        Some(BooleanFunctionShape::TextPredicate) => {
            let kind =
                boolean_text_predicate_kind(function).ok_or(PredicateCompileError::Unsupported)?;

            match kind {
                TextPredicateFunctionKind::StartsWith | TextPredicateFunctionKind::EndsWith => {
                    compile_bool_prefix_text_function_truth_predicate(kind, args, truth, work)
                }
                TextPredicateFunctionKind::Contains => {
                    compile_bool_contains_function_truth_predicate(args, truth, work)
                }
            }
        }
        Some(BooleanFunctionShape::FieldPredicate) => {
            match function
                .boolean_field_predicate_kind()
                .ok_or(PredicateCompileError::Unsupported)?
            {
                FieldPredicateFunctionKind::Missing => {
                    compile_bool_field_predicate_truth_predicate(
                        args,
                        |field| Predicate::IsMissing { field },
                        truth,
                        work,
                    )
                }
                FieldPredicateFunctionKind::Empty => compile_bool_field_predicate_truth_predicate(
                    args,
                    |field| Predicate::IsEmpty { field },
                    truth,
                    work,
                ),
                FieldPredicateFunctionKind::NotEmpty => {
                    compile_bool_field_predicate_truth_predicate(
                        args,
                        |field| Predicate::IsNotEmpty { field },
                        truth,
                        work,
                    )
                }
            }
        }
        Some(BooleanFunctionShape::CollectionContains) => {
            compile_bool_collection_contains_truth_predicate(args, truth, work)
        }
        Some(BooleanFunctionShape::Membership) => {
            compile_bool_membership_truth_predicate(args, truth, work)
        }
        Some(BooleanFunctionShape::TruthCoalesce) | None => Err(PredicateCompileError::Unsupported),
    }
}

// Resolve the finer text-predicate kind after the caller has already matched
// the broad boolean text-predicate function shape.
const fn boolean_text_predicate_kind(function: Function) -> Option<TextPredicateFunctionKind> {
    function.boolean_text_predicate_kind()
}

// Compile one null-test function onto the requested runtime null predicate
// while preserving literal-null constant behavior.
fn compile_bool_null_test_function_truth_predicate(
    kind: NullTestFunctionKind,
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [arg] = args else {
        return Err(PredicateCompileError::Unsupported);
    };

    match arg {
        Expr::Field(field) => {
            let field = work.copy_text(field.as_str())?;
            let use_null_predicate = truth.matches_bool(kind.null_matches_true());

            if use_null_predicate {
                Ok(Predicate::IsNull { field })
            } else {
                Ok(Predicate::IsNotNull { field })
            }
        }
        Expr::Literal(value) => {
            let literal_is_true = kind.null_matches_true() == matches!(value, Value::Null);

            if truth.matches_bool(literal_is_true) {
                Ok(Predicate::True)
            } else {
                Ok(Predicate::False)
            }
        }
        _ => Err(PredicateCompileError::Unsupported),
    }
}

// Compile one STARTS_WITH / ENDS_WITH boolean function onto the requested
// runtime prefix predicate over the canonical text target wrapper.
fn compile_bool_prefix_text_function_truth_predicate(
    kind: TextPredicateFunctionKind,
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        return Err(PredicateCompileError::Unsupported);
    };
    let (field, coercion) =
        compile_bool_text_target(left).ok_or(PredicateCompileError::Unsupported)?;
    let op = match kind {
        TextPredicateFunctionKind::StartsWith => CompareOp::StartsWith,
        TextPredicateFunctionKind::EndsWith => CompareOp::EndsWith,
        TextPredicateFunctionKind::Contains => return Err(PredicateCompileError::Unsupported),
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        work.copy_text(field)?,
        op,
        Value::Text(work.copy_text(value)?),
        coercion,
    ));

    wrap_truth_predicate(when_true, truth, work)
}

// Compile one CONTAINS text predicate onto the requested runtime text predicate
// shell while preserving strict versus casefold coercion.
fn compile_bool_contains_function_truth_predicate(
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [left, Expr::Literal(Value::Text(value))] = args else {
        return Err(PredicateCompileError::Unsupported);
    };
    let (field, coercion) =
        compile_bool_text_target(left).ok_or(PredicateCompileError::Unsupported)?;

    let when_true = match coercion {
        CoercionId::Strict => Predicate::TextContains {
            field: work.copy_text(field)?,
            value: Value::Text(work.copy_text(value)?),
        },
        CoercionId::TextCasefold => Predicate::TextContainsCi {
            field: work.copy_text(field)?,
            value: Value::Text(work.copy_text(value)?),
        },
        CoercionId::NumericWiden | CoercionId::CollectionElement => {
            return Err(PredicateCompileError::Unsupported);
        }
    };

    wrap_truth_predicate(when_true, truth, work)
}

// Compile one single-field boolean function onto the requested runtime
// predicate branch.
fn compile_bool_field_predicate_truth_predicate(
    args: &[Expr],
    build: impl FnOnce(String) -> Predicate,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [Expr::Field(field)] = args else {
        return Err(PredicateCompileError::Unsupported);
    };
    let when_true = build(work.copy_text(field.as_str())?);

    wrap_truth_predicate(when_true, truth, work)
}

// Compile one collection-membership boolean function onto the requested
// runtime compare predicate shell.
fn compile_bool_collection_contains_truth_predicate(
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [Expr::Field(field), Expr::Literal(value)] = args else {
        return Err(PredicateCompileError::Unsupported);
    };
    let when_true = Predicate::Compare(ComparePredicate::with_coercion(
        work.copy_text(field.as_str())?,
        CompareOp::Contains,
        work.copy_value(value)?,
        CoercionId::Strict,
    ));

    wrap_truth_predicate(when_true, truth, work)
}

// Compile compact membership without expanding it back into a large
// OR/AND tree. NULL list entries can contribute UNKNOWN, but never TRUE.
fn compile_bool_membership_truth_predicate(
    args: &[Expr],
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    let [target, Expr::Literal(Value::List(values))] = args else {
        return Err(PredicateCompileError::Unsupported);
    };
    let (field, target_coercion) =
        compile_bool_membership_target(target).ok_or(PredicateCompileError::Unsupported)?;
    let literal_set = MembershipLiteralSet::from_values(values.as_slice(), target_coercion, work)?;
    if let Some(shared_coercion) = literal_set.shared_coercion() {
        let has_null = literal_set.has_null();
        return compile_bool_compact_membership_truth_predicate(
            field,
            literal_set.into_values(),
            shared_coercion,
            has_null,
            truth,
            work,
        );
    }

    if matches!(truth, BoolTruth::True) {
        compile_bool_membership_leaf_set(
            field,
            target_coercion,
            literal_set.into_values(),
            true,
            work,
        )
    } else if literal_set.has_null() {
        Ok(Predicate::False)
    } else {
        compile_bool_membership_leaf_set(
            field,
            target_coercion,
            literal_set.into_values(),
            false,
            work,
        )
    }
}

struct MembershipLiteralSet {
    values: Vec<Value>,
    has_null: bool,
    shared_coercion: Option<CoercionId>,
}

impl MembershipLiteralSet {
    fn from_values(
        values: &[Value],
        target_coercion: Option<CoercionId>,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        let mut non_null_values = work.vec_with_capacity(values.len())?;
        let mut has_null = false;
        let mut shared_coercion = None;
        let mut requires_leaf_comparisons = false;

        for value in values {
            work.charge(Resource::NestedValueSteps, 1)?;
            if matches!(value, Value::Null) {
                has_null = true;
                continue;
            }
            // Collection-valued equality remains an ordinary leaf comparison,
            // not scalar IN coercion. Reuse the existing mixed-coercion path.
            requires_leaf_comparisons |= !membership_value_is_in_safe(value);

            let coercion =
                target_coercion.unwrap_or_else(|| compare_literal_coercion(CompareOp::Eq, value));
            match shared_coercion {
                Some(current) if current != coercion => {
                    requires_leaf_comparisons = true;
                }
                Some(_) => {}
                None => {
                    shared_coercion = Some(coercion);
                }
            }
            non_null_values.push(work.copy_value(value)?);
        }

        if non_null_values.len() < 2 || requires_leaf_comparisons {
            shared_coercion = None;
        }

        Ok(Self {
            values: non_null_values,
            has_null,
            shared_coercion,
        })
    }

    const fn has_null(&self) -> bool {
        self.has_null
    }

    const fn shared_coercion(&self) -> Option<CoercionId> {
        self.shared_coercion
    }

    fn into_values(self) -> Vec<Value> {
        self.values
    }
}

fn compile_bool_compact_membership_truth_predicate(
    field: &str,
    values: Vec<Value>,
    coercion: CoercionId,
    has_null: bool,
    truth: BoolTruth,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    if matches!(truth, BoolTruth::False) && has_null {
        return Ok(Predicate::False);
    }

    let list = canonical_membership_value_list(values);
    let op = if matches!(truth, BoolTruth::True) {
        CompareOp::In
    } else {
        CompareOp::NotIn
    };

    Ok(Predicate::Compare(ComparePredicate::with_coercion(
        work.copy_text(field)?,
        op,
        list,
        coercion,
    )))
}

fn compile_bool_membership_leaf_set(
    field: &str,
    target_coercion: Option<CoercionId>,
    values: Vec<Value>,
    positive: bool,
    work: &PreparationWork<'_>,
) -> CompileResult<Predicate> {
    if values.is_empty() {
        return Ok(if positive {
            Predicate::False
        } else {
            Predicate::True
        });
    }
    let op = if positive {
        CompareOp::Eq
    } else {
        CompareOp::Ne
    };
    let mut leaves = work.vec_with_capacity(values.len())?;
    for value in values {
        let coercion =
            target_coercion.unwrap_or_else(|| compare_literal_coercion(CompareOp::Eq, &value));
        leaves.push(Predicate::Compare(ComparePredicate::with_coercion(
            work.copy_text(field)?,
            op,
            value,
            coercion,
        )));
    }
    Ok(if positive {
        Predicate::Or(leaves)
    } else {
        Predicate::And(leaves)
    })
}

const fn compile_bool_membership_target(expr: &Expr) -> Option<(&str, Option<CoercionId>)> {
    match expr {
        Expr::Field(field) => Some((field.as_str(), None)),
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Some((field.as_str(), Some(CoercionId::TextCasefold))),
            _ => None,
        },
        _ => None,
    }
}

// Project one canonical text target wrapper onto the runtime field/coercion
// pair consumed by text predicate shells.
const fn compile_bool_text_target(expr: &Expr) -> Option<(&str, CoercionId)> {
    match expr {
        Expr::Field(field) => Some((field.as_str(), CoercionId::Strict)),
        Expr::FunctionCall {
            function: Function::Lower,
            args,
        } => match args.as_slice() {
            [Expr::Field(field)] => Some((field.as_str(), CoercionId::TextCasefold)),
            _ => None,
        },
        _ => None,
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
            // CASE expansion belongs to canonicalization. A retained CASE must
            // stay expression-backed rather than bypass its rewrite budget here.
            Expr::Case { .. } => false,
            Expr::FieldPath(_) | Expr::Aggregate(_) | Expr::Literal(_) => false,
            #[cfg(test)]
            Expr::Alias { .. } => false,
        }
    }

    // Admit only normalized compare shapes that lower directly onto runtime
    // predicate compare shells.
    const fn is_compare_expr(op: BinaryOp, left: &Expr, right: &Expr) -> bool {
        if truth_condition_binary_compare_op(op).is_none() {
            return false;
        }

        match (left, right) {
            (Expr::Field(_), Expr::Literal(_) | Expr::Field(_))
            | (Expr::FieldPath(_), Expr::Literal(_)) => true,
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
            Some(BooleanFunctionShape::Membership) => {
                matches!(
                    args,
                    [target, Expr::Literal(Value::List(values))]
                        if Self::is_membership_target(target)
                            && membership_values_are_predicate_admissible(target, values)
                )
            }
            Some(BooleanFunctionShape::TruthCoalesce) | None => false,
        }
    }

    // Admit only canonical text targets that map directly onto runtime text
    // predicate shells.
    const fn is_text_target(expr: &Expr) -> bool {
        match expr {
            Expr::Field(_) => true,
            Expr::FunctionCall {
                function: Function::Lower,
                args,
            } => matches!(args.as_slice(), [Expr::Field(_)]),
            _ => false,
        }
    }

    const fn is_membership_target(expr: &Expr) -> bool {
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

fn membership_values_are_predicate_admissible(target: &Expr, values: &[Value]) -> bool {
    let casefold = matches!(
        target,
        Expr::FunctionCall {
            function: Function::Lower,
            ..
        }
    );

    !casefold
        || values
            .iter()
            .all(|value| matches!(value, Value::Null | Value::Text(_)))
}

const fn compare_literal_coercion(op: CompareOp, value: &Value) -> CoercionId {
    match value {
        Value::Text(_) | Value::Nat64(_) | Value::Nat128(_) | Value::NatBig(_) => {
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

const fn compare_field_coercion(op: CompareOp) -> Option<CoercionId> {
    if !op.supports_field_compare() {
        return None;
    }

    Some(if op.is_ordering_family() {
        CoercionId::NumericWiden
    } else {
        CoercionId::Strict
    })
}
