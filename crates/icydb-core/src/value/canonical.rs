//! Module: value::canonical
//! Responsibility: deterministic canonical ordering helpers for dynamic values.
//! Does not own: predicate normalization, access planning, or storage layout.
//! Boundary: shared value-level canonicalization used by higher db layers.

use crate::value::Value;
use std::{borrow::Borrow, cmp::Ordering};

/// Canonicalize one value set with deterministic order + dedup semantics.
/// Owned values and borrowed encoding views use the same value comparison.
pub(crate) fn canonicalize_value_set<T: Borrow<Value>>(values: &mut Vec<T>) {
    if value_set_is_strictly_canonical(values.as_slice()) {
        return;
    }

    values.sort_unstable_by(|left, right| Value::canonical_cmp(left.borrow(), right.borrow()));
    values.dedup_by(|left, right| T::borrow(left) == T::borrow(right));
}

fn value_set_is_strictly_canonical<T: Borrow<Value>>(values: &[T]) -> bool {
    values.windows(2).all(|pair| {
        matches!(
            Value::canonical_cmp(pair[0].borrow(), pair[1].borrow()),
            Ordering::Less
        )
    })
}
