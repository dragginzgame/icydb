use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, eval_equality_compare_result,
        eval_list_membership_compare_result, eval_ordered_compare_result,
    },
    value::Value,
};

// Evaluate one strict scalar compare directly against the predicate literal and
// literal lists, leaving only unsupported coercions on the generic fallback.
pub(in crate::db::predicate::runtime::compare::scalar) fn eval_direct_scalar_compare<T>(
    actual: T,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
    decode: impl Fn(&Value) -> Option<T>,
) -> Option<bool>
where
    T: Copy + Eq + Ord,
{
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq
            | CompareOp::Ne
            | CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte => Some(eval_ordered_scalar_compare(actual, op, value, decode)),
            CompareOp::In | CompareOp::NotIn => Some(eval_list_membership_compare_result(
                op,
                scalar_in_list(actual, value, decode),
            )),
            CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => Some(false),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

// Evaluate one ordered scalar literal compare after decoding the predicate
// literal exactly once for the whole compare branch.
fn eval_ordered_scalar_compare<T>(
    actual: T,
    op: CompareOp,
    value: &Value,
    decode: impl Fn(&Value) -> Option<T>,
) -> bool
where
    T: Copy + Ord,
{
    let Some(expected) = decode(value) else {
        return false;
    };

    eval_ordered_compare_result(op, actual.cmp(&expected))
}

// Evaluate direct blob equality/list membership without rebuilding `Value::Blob`.
pub(in crate::db::predicate::runtime::compare::scalar) fn eval_blob_scalar_compare(
    actual: &[u8],
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq | CompareOp::Ne => {
                Some(eval_equality_compare_result(op, blob_eq(actual, value)))
            }
            CompareOp::In | CompareOp::NotIn => Some(eval_list_membership_compare_result(
                op,
                blob_in_list(actual, value),
            )),
            CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => Some(false),
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

// Evaluate direct null comparisons without rebuilding `Value::Null`.
pub(in crate::db::predicate::runtime::compare::scalar) fn eval_null_scalar_compare(
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq | CompareOp::Ne => Some(eval_equality_compare_result(op, null_eq(value))),
            CompareOp::Lt
            | CompareOp::Lte
            | CompareOp::Gt
            | CompareOp::Gte
            | CompareOp::Contains
            | CompareOp::StartsWith
            | CompareOp::EndsWith => Some(false),
            CompareOp::In | CompareOp::NotIn => {
                Some(eval_list_membership_compare_result(op, null_in_list(value)))
            }
        },
        CoercionId::TextCasefold => Some(false),
        CoercionId::NumericWiden => None,
    }
}

fn scalar_in_list<T>(actual: T, list: &Value, decode: impl Fn(&Value) -> Option<T>) -> Option<bool>
where
    T: Copy + Eq,
{
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Some(expected) = decode(item) {
            if actual == expected {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn blob_eq(actual: &[u8], value: &Value) -> Option<bool> {
    match value {
        Value::Blob(expected) => Some(actual == expected.as_slice()),
        _ => None,
    }
}

fn blob_in_list(actual: &[u8], list: &Value) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        if let Value::Blob(expected) = item {
            if actual == expected.as_slice() {
                return Some(true);
            }
            saw_valid = true;
        }
    }

    saw_valid.then_some(false)
}

fn null_eq(value: &Value) -> Option<bool> {
    matches!(value, Value::Null).then_some(true)
}

fn null_in_list(list: &Value) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    for item in items {
        if matches!(item, Value::Null) {
            return Some(true);
        }
    }

    None
}
