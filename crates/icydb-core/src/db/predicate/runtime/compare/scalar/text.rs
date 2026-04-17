use crate::{
    db::predicate::{CoercionId, CoercionSpec, CompareOp},
    value::{TextMode, Value},
};
use std::cmp::Ordering;

use crate::db::predicate::runtime::compare::scalar::direct::eval_ordered_compare_result;

// Evaluate one scalar text compare without allocating an owned `Value::Text`.
pub(super) fn eval_text_scalar_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    let mode = match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        CoercionId::NumericWiden => return None,
    };

    match op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => Some(eval_text_scalar_order_compare(actual, op, value, mode)),
        CompareOp::StartsWith => Some(
            matches!(value, Value::Text(expected) if text_starts_with_scalar(actual, expected, mode)),
        ),
        CompareOp::EndsWith => Some(
            matches!(value, Value::Text(expected) if text_ends_with_scalar(actual, expected, mode)),
        ),
        CompareOp::In | CompareOp::NotIn => {
            Some(eval_text_list_membership_compare(op, actual, value, mode))
        }
        CompareOp::Contains => Some(false),
    }
}

// Evaluate one ordered text compare against one scalar text value without
// repeating the literal-match and canonical text compare path for each op.
fn eval_text_scalar_order_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    mode: TextMode,
) -> bool {
    let Value::Text(expected) = value else {
        return false;
    };

    eval_ordered_compare_result(op, compare_scalar_text(actual, expected, mode))
}

fn eval_text_list_membership_compare(
    op: CompareOp,
    actual: &str,
    value: &Value,
    mode: TextMode,
) -> bool {
    match op {
        CompareOp::In => text_in_list(actual, value, mode).unwrap_or(false),
        CompareOp::NotIn => text_in_list(actual, value, mode).is_some_and(|did_match| !did_match),
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
    }
}

fn text_in_list(actual: &str, list: &Value, mode: TextMode) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Value::Text(expected) = item {
            if compare_scalar_text(actual, expected, mode) == Ordering::Equal {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn compare_scalar_text(actual: &str, expected: &str, mode: TextMode) -> Ordering {
    match mode {
        TextMode::Cs => actual.cmp(expected),
        TextMode::Ci => casefold_scalar_text(actual).cmp(&casefold_scalar_text(expected)),
    }
}

pub(in crate::db::predicate::runtime) fn text_contains_scalar(
    actual: &str,
    needle: &str,
    mode: TextMode,
) -> bool {
    match mode {
        TextMode::Cs => actual.contains(needle),
        TextMode::Ci => casefold_scalar_text(actual).contains(&casefold_scalar_text(needle)),
    }
}

fn text_starts_with_scalar(actual: &str, prefix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.starts_with(prefix),
        TextMode::Ci => casefold_scalar_text(actual).starts_with(&casefold_scalar_text(prefix)),
    }
}

fn text_ends_with_scalar(actual: &str, suffix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.ends_with(suffix),
        TextMode::Ci => casefold_scalar_text(actual).ends_with(&casefold_scalar_text(suffix)),
    }
}

fn casefold_scalar_text(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}
