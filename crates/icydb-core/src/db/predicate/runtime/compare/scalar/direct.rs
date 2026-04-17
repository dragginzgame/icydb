use crate::{
    db::predicate::{CoercionId, CoercionSpec, CompareOp},
    value::Value,
};
use std::cmp::Ordering;

// Evaluate one strict scalar compare directly against the predicate literal and
// literal lists, leaving only unsupported coercions on the generic fallback.
pub(super) fn eval_direct_scalar_compare<T>(
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
pub(super) fn eval_blob_scalar_compare(
    actual: &[u8],
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => {
                Some(matches!(value, Value::Blob(expected) if actual == expected.as_slice()))
            }
            CompareOp::Ne => {
                Some(matches!(value, Value::Blob(expected) if actual != expected.as_slice()))
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
pub(super) fn eval_null_scalar_compare(
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => match op {
            CompareOp::Eq => Some(matches!(value, Value::Null)),
            CompareOp::Ne
            | CompareOp::Lt
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

// Share the ordered compare-op mapping across direct scalar and text fast
// paths so each caller only owns literal decode / canonical compare work.
pub(super) fn eval_ordered_compare_result(op: CompareOp, ordering: Ordering) -> bool {
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering.is_lt(),
        CompareOp::Lte => ordering.is_le(),
        CompareOp::Gt => ordering.is_gt(),
        CompareOp::Gte => ordering.is_ge(),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => false,
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

// Keep `IN` / `NOT IN` result shaping identical across scalar fast-path
// variants after each lane has evaluated its list-membership semantics.
fn eval_list_membership_compare_result(op: CompareOp, matched: Option<bool>) -> bool {
    match op {
        CompareOp::In => matched.unwrap_or(false),
        CompareOp::NotIn => matched.is_some_and(|did_match| !did_match),
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

pub(super) const fn scalar_bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_date_from_value(value: &Value) -> Option<crate::types::Date> {
    match value {
        Value::Date(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_duration_from_value(value: &Value) -> Option<crate::types::Duration> {
    match value {
        Value::Duration(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_float32_from_value(value: &Value) -> Option<crate::types::Float32> {
    match value {
        Value::Float32(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_float64_from_value(value: &Value) -> Option<crate::types::Float64> {
    match value {
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_int_from_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_principal_from_value(value: &Value) -> Option<crate::types::Principal> {
    match value {
        Value::Principal(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_subaccount_from_value(
    value: &Value,
) -> Option<crate::types::Subaccount> {
    match value {
        Value::Subaccount(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_timestamp_from_value(value: &Value) -> Option<crate::types::Timestamp> {
    match value {
        Value::Timestamp(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_uint_from_value(value: &Value) -> Option<u64> {
    match value {
        Value::Uint(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_ulid_from_value(value: &Value) -> Option<crate::types::Ulid> {
    match value {
        Value::Ulid(value) => Some(*value),
        _ => None,
    }
}

pub(super) const fn scalar_unit_from_value(value: &Value) -> Option<()> {
    match value {
        Value::Unit => Some(()),
        _ => None,
    }
}
