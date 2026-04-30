//! Module: value::ops::ordering
//!
//! Responsibility: ordering helpers that are behavioral rather than identity data.
//! Does not own: canonical variant rank/tag definitions.
//! Boundary: map-entry and strict-order helpers shared by runtime consumers.

use crate::value::{Value, compare, ops::numeric, semantics};
use std::cmp::Ordering;

/// Total canonical comparator for one map entry `(key, value)`.
#[must_use]
pub fn canonical_cmp_map_entry(
    left_key: &Value,
    left_value: &Value,
    right_key: &Value,
    right_value: &Value,
) -> Ordering {
    Value::canonical_cmp_key(left_key, right_key)
        .then_with(|| Value::canonical_cmp(left_value, right_value))
}

/// Build one borrowed canonical map-entry order for hashing and
/// fingerprint-adjacent encoding surfaces.
#[must_use]
pub fn ordered_map_entries(entries: &[(Value, Value)]) -> Vec<&(Value, Value)> {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| canonical_cmp_map_entry(&left.0, &left.1, &right.0, &right.1));

    ordered
}

/// Strict comparator for identical orderable variants.
#[must_use]
pub(crate) fn strict_order_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    compare::strict_order_cmp(left, right)
}

/// Compare two values for projection-style equality.
#[must_use]
pub(crate) fn eq(left: &Value, right: &Value) -> Option<bool> {
    let numeric_widen_enabled =
        semantics::supports_numeric_coercion(left) || semantics::supports_numeric_coercion(right);
    if numeric_widen_enabled {
        return numeric::compare_decimal_order(left, right)
            .map(|ordering| ordering == Ordering::Equal);
    }

    Some(left == right)
}

/// Compare two values for projection-style inequality.
#[must_use]
pub(crate) fn ne(left: &Value, right: &Value) -> Option<bool> {
    eq(left, right).map(|equal| !equal)
}

/// Compare two values under projection-style ordering semantics.
#[must_use]
pub(crate) fn order(left: &Value, right: &Value) -> Option<Ordering> {
    let numeric_widen_enabled =
        semantics::supports_numeric_coercion(left) || semantics::supports_numeric_coercion(right);
    if numeric_widen_enabled {
        return numeric::compare_decimal_order(left, right);
    }

    strict_order_cmp(left, right)
}

/// Return whether `left < right` under projection-style ordering semantics.
#[must_use]
pub(crate) fn lt(left: &Value, right: &Value) -> Option<bool> {
    order(left, right).map(Ordering::is_lt)
}

/// Return whether `left <= right` under projection-style ordering semantics.
#[must_use]
pub(crate) fn lte(left: &Value, right: &Value) -> Option<bool> {
    order(left, right).map(Ordering::is_le)
}

/// Return whether `left > right` under projection-style ordering semantics.
#[must_use]
pub(crate) fn gt(left: &Value, right: &Value) -> Option<bool> {
    order(left, right).map(Ordering::is_gt)
}

/// Return whether `left >= right` under projection-style ordering semantics.
#[must_use]
pub(crate) fn gte(left: &Value, right: &Value) -> Option<bool> {
    order(left, right).map(Ordering::is_ge)
}

impl Value {
    /// Total canonical comparator for one map entry `(key, value)`.
    ///
    /// This keeps map-entry ordering aligned across normalization, hashing,
    /// and fingerprint-adjacent surfaces.
    #[must_use]
    pub(crate) fn canonical_cmp_map_entry(
        left_key: &Self,
        left_value: &Self,
        right_key: &Self,
        right_value: &Self,
    ) -> Ordering {
        canonical_cmp_map_entry(left_key, left_value, right_key, right_value)
    }

    /// Build one borrowed canonical map-entry order for hashing and
    /// fingerprint-adjacent encoding surfaces.
    #[must_use]
    pub(crate) fn ordered_map_entries(entries: &[(Self, Self)]) -> Vec<&(Self, Self)> {
        ordered_map_entries(entries)
    }

    /// Strict comparator for identical orderable variants.
    ///
    /// Returns `None` for mismatched or non-orderable variants.
    #[must_use]
    pub(crate) fn strict_order_cmp(left: &Self, right: &Self) -> Option<Ordering> {
        strict_order_cmp(left, right)
    }
}
