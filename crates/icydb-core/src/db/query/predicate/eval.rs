use crate::{
    db::query::predicate::{
        CompareOp, ComparePredicate, Predicate,
        coercion::{CoercionSpec, TextOp, compare_eq, compare_order, compare_text},
    },
    traits::FieldValues,
    value::{TextMode, Value},
};
use std::cmp::Ordering;

///
/// FieldPresence
///
/// Result of attempting to read a field from a row during predicate
/// evaluation. This distinguishes between a missing field and a
/// present field whose value may be `None`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldPresence {
    /// Field exists and has a value (including `Value::Null`).
    Present(Value),
    /// Field is not present on the row.
    Missing,
}

///
/// Row
///
/// Abstraction over a row-like value that can expose fields by name.
/// This decouples predicate evaluation from concrete entity types.
///

pub(crate) trait Row {
    fn field(&self, name: &str) -> FieldPresence;
}

///
/// Default `Row` implementation for any type that exposes
/// `FieldValues`, which is the standard runtime entity interface.
///

impl<T: FieldValues> Row for T {
    fn field(&self, name: &str) -> FieldPresence {
        match self.get_value(name) {
            Some(value) => FieldPresence::Present(value),
            None => FieldPresence::Missing,
        }
    }
}

// Evaluate a field predicate only when the field is present.
fn on_present<R: Row + ?Sized>(row: &R, field: &str, f: impl FnOnce(&Value) -> bool) -> bool {
    match row.field(field) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

///
/// Evaluate a predicate against a single row.
///
/// This function performs **pure runtime evaluation**:
/// - no schema access
/// - no planning or index logic
/// - no validation
///
/// Any unsupported comparison simply evaluates to `false`.
/// CONTRACT: internal-only; predicates must be validated before evaluation.
///
#[must_use]
#[expect(clippy::match_like_matches_macro)]
pub(crate) fn eval<R: Row + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,

        Predicate::And(children) => children.iter().all(|child| eval(row, child)),
        Predicate::Or(children) => children.iter().any(|child| eval(row, child)),
        Predicate::Not(inner) => !eval(row, inner),

        Predicate::Compare(cmp) => eval_compare(row, cmp),

        Predicate::IsNull { field } => match row.field(field) {
            FieldPresence::Present(Value::Null) => true,
            _ => false,
        },

        Predicate::IsMissing { field } => matches!(row.field(field), FieldPresence::Missing),

        Predicate::IsEmpty { field } => on_present(row, field, is_empty_value),

        Predicate::IsNotEmpty { field } => on_present(row, field, |value| !is_empty_value(value)),
        Predicate::TextContains { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Cs).unwrap_or(false)
        }),
        Predicate::TextContainsCi { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Ci).unwrap_or(false)
        }),
    }
}

///
/// Evaluate a single comparison predicate against a row.
///
/// Returns `false` if:
/// - the field is missing
/// - the comparison is not defined under the given coercion
///
fn eval_compare<R: Row + ?Sized>(row: &R, cmp: &ComparePredicate) -> bool {
    let ComparePredicate {
        field,
        op,
        value,
        coercion,
    } = cmp;

    let FieldPresence::Present(actual) = row.field(field) else {
        return false;
    };

    // NOTE: Comparison helpers return None when a comparison is invalid; eval treats that as false.
    match op {
        CompareOp::Eq => compare_eq(&actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(&actual, value, coercion).is_some_and(|v| !v),

        CompareOp::Lt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_ge),

        CompareOp::In => in_list(&actual, value, coercion).unwrap_or(false),
        CompareOp::NotIn => in_list(&actual, value, coercion).is_some_and(|matched| !matched),

        CompareOp::Contains => contains(&actual, value, coercion),

        CompareOp::StartsWith => {
            compare_text(&actual, value, coercion, TextOp::StartsWith).unwrap_or(false)
        }
        CompareOp::EndsWith => {
            compare_text(&actual, value, coercion, TextOp::EndsWith).unwrap_or(false)
        }
    }
}

///
/// Determine whether a value is considered empty for `IsEmpty` checks.
///
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

///
/// Check whether a value equals any element in a list.
///
fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        match compare_eq(actual, item, coercion) {
            Some(true) => return Some(true),
            Some(false) => saw_valid = true,
            None => {}
        }
    }

    saw_valid.then_some(false)
}

///
/// Check whether a collection contains another value.
///
/// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
///
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if matches!(actual, Value::Text(_)) {
        // CONTRACT: text substring matching uses TextContains/TextContainsCi.
        return false;
    }

    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        // Invalid comparisons are treated as non-matches.
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}
