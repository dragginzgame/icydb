//! Module: value::ops::partial_ord
//!
//! Responsibility: Rust `PartialOrd` implementation for dynamic values.
//! Does not own: canonical ordering or predicate-level ordering semantics.
//! Boundary: compatibility implementation for value-local partial comparison.

use crate::value::Value;
use std::cmp::Ordering;

// NOTE:
// Value::partial_cmp is NOT the canonical ordering for database semantics.
// Some orderable scalar types (e.g. Account, Unit) intentionally do not
// participate here. Use canonical_cmp / strict ordering for ORDER BY,
// planning, and key-range validation.
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::Date(a), Self::Date(b)) => a.partial_cmp(b),
            (Self::Decimal(a), Self::Decimal(b)) => a.partial_cmp(b),
            (Self::Duration(a), Self::Duration(b)) => a.partial_cmp(b),
            (Self::Enum(a), Self::Enum(b)) => a.partial_cmp(b),
            (Self::Float32(a), Self::Float32(b)) => a.partial_cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.partial_cmp(b),
            (Self::Int(a), Self::Int(b)) => a.partial_cmp(b),
            (Self::Int128(a), Self::Int128(b)) => a.partial_cmp(b),
            (Self::IntBig(a), Self::IntBig(b)) => a.partial_cmp(b),
            (Self::Principal(a), Self::Principal(b)) => a.partial_cmp(b),
            (Self::Subaccount(a), Self::Subaccount(b)) => a.partial_cmp(b),
            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
            (Self::Nat(a), Self::Nat(b)) => a.partial_cmp(b),
            (Self::Nat128(a), Self::Nat128(b)) => a.partial_cmp(b),
            (Self::NatBig(a), Self::NatBig(b)) => a.partial_cmp(b),
            (Self::Ulid(a), Self::Ulid(b)) => a.partial_cmp(b),
            (Self::Map(a), Self::Map(b)) => partial_cmp_map(a.as_slice(), b.as_slice()),

            // Cross-type comparisons: no ordering
            _ => None,
        }
    }
}

fn partial_cmp_map(left: &[(Value, Value)], right: &[(Value, Value)]) -> Option<Ordering> {
    for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right.iter()) {
        let key_cmp = Value::canonical_cmp_key(left_key, right_key);
        if key_cmp != Ordering::Equal {
            return Some(key_cmp);
        }

        match left_value.partial_cmp(right_value) {
            Some(Ordering::Equal) => {}
            non_eq => return non_eq,
        }
    }

    left.len().partial_cmp(&right.len())
}
