//! Module: query::plan::expr::predicate::bridge
//! Responsibility: convert runtime predicates into planner-owned boolean
//! expressions before routing them through canonicalization.
//! Does not own: predicate compilation, type inference, projection evaluation,
//! or scalar expression execution.
//! Boundary: bridges runtime predicate shapes into canonical boolean IR;
//! predicate compilation remains the one-way lowering stage from canonical IR
//! back to runtime predicates.

use crate::{
    db::{
        predicate::{CoercionId, CompareFieldsPredicate, CompareOp, ComparePredicate, Predicate},
        query::plan::expr::{
            BinaryOp, Expr, FieldId, Function, UnaryOp, is_normalized_bool_expr,
            normalize_bool_expr, truth_condition_compare_binary_op,
        },
    },
    value::Value,
};

#[cfg(test)]
use crate::db::query::plan::expr::compile_normalized_bool_expr_to_predicate;

/// Canonicalize one runtime predicate by routing it through the planner-owned
/// boolean-expression seam and rebuilding the runtime predicate shell from the
/// normalized planner form.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn canonicalize_runtime_predicate_via_bool_expr(
    predicate: Predicate,
) -> Predicate {
    let expr = predicate_to_bool_expr(&predicate);
    let expr = normalize_bool_expr(expr);

    debug_assert!(is_normalized_bool_expr(&expr));

    compile_normalized_bool_expr_to_predicate(&expr)
}

/// Test-only export for the runtime-predicate -> planner-expression bridge.
#[must_use]
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
                .expect("predicate bridge invariant"),
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
        op: truth_condition_compare_binary_op(compare.op()).expect("predicate bridge invariant"),
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

// Convert one runtime membership compare onto the compact planner membership
// function consumed by shared predicate compilation.
fn membership_compare_predicate_to_bool_expr(compare: &ComparePredicate) -> Expr {
    let values = match compare.value() {
        Value::List(values) => values.as_slice(),
        _ => return Expr::Literal(Value::Bool(matches!(compare.op(), CompareOp::NotIn))),
    };
    let expr = Expr::FunctionCall {
        function: Function::InList,
        args: vec![
            casefold_field_expr(compare.field(), compare.coercion().id()),
            Expr::Literal(Value::List(values.to_vec())),
        ],
    };

    if matches!(compare.op(), CompareOp::NotIn) {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        }
    } else {
        expr
    }
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
