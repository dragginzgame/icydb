use super::{
    ast::{CompareOp, ComparePredicate, Predicate},
    coercion::{CoercionSpec, TextOp, compare_eq, compare_order, compare_text},
};
use crate::{traits::FieldValues, value::Value};
use std::cmp::Ordering;

///
/// FieldPresence
///
/// Result of attempting to read a field from a row during predicate
/// evaluation. This distinguishes between a missing field and a
/// present field whose value may be `None`.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldPresence {
    /// Field exists and has a value (including `Value::None`).
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
pub trait Row {
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

///
/// Evaluate a predicate against a single row.
///
/// This function performs **pure runtime evaluation**:
/// - no schema access
/// - no planning or index logic
/// - no validation
///
/// Any unsupported comparison simply evaluates to `false`.
///
#[must_use]
#[expect(clippy::match_like_matches_macro)]
pub fn eval<R: Row + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,

        Predicate::And(children) => children.iter().all(|child| eval(row, child)),
        Predicate::Or(children) => children.iter().any(|child| eval(row, child)),
        Predicate::Not(inner) => !eval(row, inner),

        Predicate::Compare(cmp) => eval_compare(row, cmp),

        Predicate::IsNull { field } => match row.field(field) {
            FieldPresence::Present(Value::None) => true,
            _ => false,
        },

        Predicate::IsMissing { field } => matches!(row.field(field), FieldPresence::Missing),

        Predicate::IsEmpty { field } => match row.field(field) {
            FieldPresence::Present(value) => is_empty_value(&value),
            FieldPresence::Missing => false,
        },

        Predicate::IsNotEmpty { field } => match row.field(field) {
            FieldPresence::Present(value) => !is_empty_value(&value),
            FieldPresence::Missing => false,
        },

        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(value) => map_contains_key(&value, key, coercion),
            FieldPresence::Missing => false,
        },

        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(actual) => map_contains_value(&actual, value, coercion),
            FieldPresence::Missing => false,
        },

        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(actual) => map_contains_entry(&actual, key, value, coercion),
            FieldPresence::Missing => false,
        },
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

    match op {
        CompareOp::Eq => compare_eq(&actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(&actual, value, coercion).is_some_and(|v| !v),

        CompareOp::Lt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_ge),

        CompareOp::In => in_list(&actual, value, coercion),
        CompareOp::NotIn => !in_list(&actual, value, coercion),

        CompareOp::AnyIn => any_in(&actual, value, coercion),
        CompareOp::AllIn => all_in(&actual, value, coercion),

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
fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(items) = list else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(actual, item, coercion).unwrap_or(false))
}

///
/// Check whether any element of `actual` exists in `list`.
///
fn any_in(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(actual_items) = actual else {
        return false;
    };
    let Value::List(needles) = list else {
        return false;
    };

    actual_items.iter().any(|item| {
        needles
            .iter()
            .any(|needle| compare_eq(item, needle, coercion).unwrap_or(false))
    })
}

///
/// Check whether all elements of `actual` exist in `list`.
///
fn all_in(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(actual_items) = actual else {
        return false;
    };
    let Value::List(needles) = list else {
        return false;
    };

    actual_items.iter().all(|item| {
        needles
            .iter()
            .any(|needle| compare_eq(item, needle, coercion).unwrap_or(false))
    })
}

///
/// Check whether a value contains another value.
///
/// For textual values, this defers to text comparison semantics.
/// For collections, this performs element-wise equality checks.
///
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if let Some(res) = compare_text(actual, needle, coercion, TextOp::Contains) {
        return res;
    }

    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

///
/// Check whether a map-like value contains a given key.
///
/// Maps are represented as lists of 2-element lists `[key, value]`.
///
fn map_contains_key(map: &Value, key: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }
        if compare_eq(&pair[0], key, coercion).unwrap_or(false) {
            return true;
        }
    }

    false
}

///
/// Check whether a map-like value contains a given value.
///
fn map_contains_value(map: &Value, value: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }
        if compare_eq(&pair[1], value, coercion).unwrap_or(false) {
            return true;
        }
    }

    false
}

///
/// Check whether a map-like value contains an exact key/value pair.
///
fn map_contains_entry(map: &Value, key: &Value, value: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }

        let key_match = compare_eq(&pair[0], key, coercion).unwrap_or(false);
        let value_match = compare_eq(&pair[1], value, coercion).unwrap_or(false);

        if key_match && value_match {
            return true;
        }
    }

    false
}
