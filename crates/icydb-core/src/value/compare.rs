use crate::value::{Value, ValueEnum};
use std::cmp::Ordering;

/// Total canonical comparator used by planner/predicate/fingerprint surfaces.
///
/// Ordering rules:
/// 1. Canonical variant rank
/// 2. Variant-specific comparison for same-ranked values
///
/// Mixed-variant comparisons are rank-only and must remain deterministic.
#[must_use]
pub fn canonical_cmp(left: &Value, right: &Value) -> Ordering {
    let rank = left.canonical_rank().cmp(&right.canonical_rank());
    if rank != Ordering::Equal {
        return rank;
    }

    canonical_cmp_same_rank(left, right)
}

/// Total canonical comparator used for map-key normalization.
#[must_use]
pub fn canonical_cmp_key(left: &Value, right: &Value) -> Ordering {
    canonical_cmp(left, right)
}

/// Strict comparator for identical orderable variants.
///
/// Returns `None` for mismatched or non-orderable variants.
#[must_use]
pub fn strict_order_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Account(a), Value::Account(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
        (Value::Duration(a), Value::Duration(b)) => a.partial_cmp(b),
        (Value::E8s(a), Value::E8s(b)) => a.partial_cmp(b),
        (Value::E18s(a), Value::E18s(b)) => a.partial_cmp(b),
        (Value::Enum(a), Value::Enum(b)) => a.partial_cmp(b),
        (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::Int128(a), Value::Int128(b)) => a.partial_cmp(b),
        (Value::IntBig(a), Value::IntBig(b)) => a.partial_cmp(b),
        (Value::Map(a), Value::Map(b)) => strict_order_map(a.as_slice(), b.as_slice()),
        (Value::Principal(a), Value::Principal(b)) => a.partial_cmp(b),
        (Value::Subaccount(a), Value::Subaccount(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Uint(a), Value::Uint(b)) => a.partial_cmp(b),
        (Value::Uint128(a), Value::Uint128(b)) => a.partial_cmp(b),
        (Value::UintBig(a), Value::UintBig(b)) => a.partial_cmp(b),
        (Value::Ulid(a), Value::Ulid(b)) => a.partial_cmp(b),
        (Value::Unit, Value::Unit) => Some(Ordering::Equal),
        _ => None,
    }
}

fn canonical_cmp_same_rank(left: &Value, right: &Value) -> Ordering {
    #[allow(clippy::match_same_arms)]
    match (left, right) {
        (Value::Account(a), Value::Account(b)) => a.cmp(b),
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
        (Value::Duration(a), Value::Duration(b)) => a.cmp(b),
        (Value::Enum(a), Value::Enum(b)) => canonical_cmp_value_enum(a, b),
        (Value::E8s(a), Value::E8s(b)) => a.cmp(b),
        (Value::E18s(a), Value::E18s(b)) => a.cmp(b),
        (Value::Float32(a), Value::Float32(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Int128(a), Value::Int128(b)) => a.cmp(b),
        (Value::IntBig(a), Value::IntBig(b)) => a.cmp(b),
        (Value::List(a), Value::List(b)) => canonical_cmp_value_list(a, b),
        (Value::Map(a), Value::Map(b)) => canonical_cmp_value_map(a, b),
        (Value::Principal(a), Value::Principal(b)) => a.cmp(b),
        (Value::Subaccount(a), Value::Subaccount(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::Uint(a), Value::Uint(b)) => a.cmp(b),
        (Value::Uint128(a), Value::Uint128(b)) => a.cmp(b),
        (Value::UintBig(a), Value::UintBig(b)) => a.cmp(b),
        (Value::Ulid(a), Value::Ulid(b)) => a.cmp(b),
        (Value::Null, Value::Null) | (Value::Unit, Value::Unit) => Ordering::Equal,
        _ => Ordering::Equal,
    }
}

fn canonical_cmp_value_list(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let cmp = canonical_cmp(left, right);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    left.len().cmp(&right.len())
}

fn canonical_cmp_value_map(left: &[(Value, Value)], right: &[(Value, Value)]) -> Ordering {
    for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right.iter()) {
        let key_cmp = canonical_cmp(left_key, right_key);
        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        let value_cmp = canonical_cmp(left_value, right_value);
        if value_cmp != Ordering::Equal {
            return value_cmp;
        }
    }

    left.len().cmp(&right.len())
}

fn canonical_cmp_value_enum(left: &ValueEnum, right: &ValueEnum) -> Ordering {
    let cmp = left.variant.cmp(&right.variant);
    if cmp != Ordering::Equal {
        return cmp;
    }

    let cmp = left.path.cmp(&right.path);
    if cmp != Ordering::Equal {
        return cmp;
    }

    match (&left.payload, &right.payload) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => canonical_cmp(left, right),
    }
}

// Recursively compare map entries under strict-order semantics.
fn strict_order_map(left: &[(Value, Value)], right: &[(Value, Value)]) -> Option<Ordering> {
    let limit = left.len().min(right.len());
    for ((left_key, left_value), (right_key, right_value)) in
        left.iter().zip(right.iter()).take(limit)
    {
        let key_cmp = canonical_cmp(left_key, right_key);
        if key_cmp != Ordering::Equal {
            return Some(key_cmp);
        }

        let value_cmp = strict_order_cmp(left_value, right_value)?;
        if value_cmp != Ordering::Equal {
            return Some(value_cmp);
        }
    }

    left.len().partial_cmp(&right.len())
}
