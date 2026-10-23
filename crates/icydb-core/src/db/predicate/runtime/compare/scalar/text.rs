use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, eval_list_membership_compare_result,
        eval_ordered_compare_result,
    },
    value::{TextMode, Value, casefold_text},
};
use std::{borrow::Cow, cmp::Ordering};

// Runtime programs prepare casefolded literals once; only the row text needs
// folding here, including when an IN list visits several literal candidates.
pub(in crate::db::predicate::runtime) fn eval_text_scalar_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    let actual = match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => Cow::Borrowed(actual),
        CoercionId::TextCasefold => Cow::Owned(casefold_text(actual)),
        CoercionId::NumericWiden => return None,
    };

    match op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => Some(eval_text_scalar_order_compare(&actual, op, value)),
        CompareOp::StartsWith => {
            Some(matches!(value, Value::Text(expected) if actual.starts_with(expected)))
        }
        CompareOp::EndsWith => {
            Some(matches!(value, Value::Text(expected) if actual.ends_with(expected)))
        }
        CompareOp::In | CompareOp::NotIn => {
            Some(eval_text_list_membership_compare(op, &actual, value))
        }
        CompareOp::Contains => Some(false),
    }
}

// Evaluate one ordered text compare against one scalar text value without
// repeating the literal-match and canonical text compare path for each op.
fn eval_text_scalar_order_compare(actual: &str, op: CompareOp, value: &Value) -> bool {
    let Value::Text(expected) = value else {
        return false;
    };

    eval_ordered_compare_result(op, actual.cmp(expected))
}

fn eval_text_list_membership_compare(op: CompareOp, actual: &str, value: &Value) -> bool {
    eval_list_membership_compare_result(op, text_in_list(actual, value))
}

fn text_in_list(actual: &str, list: &Value) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Value::Text(expected) = item {
            if actual.cmp(expected) == Ordering::Equal {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

// As with compare literals, a CI needle is folded during program compilation.
pub(in crate::db::predicate::runtime) fn text_contains_scalar(
    actual: &str,
    needle: &str,
    mode: TextMode,
) -> bool {
    match mode {
        TextMode::Cs => actual.contains(needle),
        TextMode::Ci => casefold_text(actual).contains(needle),
    }
}
