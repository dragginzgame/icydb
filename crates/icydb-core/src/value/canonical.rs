//! Module: value::canonical
//! Responsibility: deterministic canonical ordering helpers for dynamic values.
//! Does not own: predicate normalization, access planning, or storage layout.
//! Boundary: shared value-level canonicalization used by higher db layers.

use crate::value::Value;
use std::cmp::Ordering;

/// Canonicalize one value set with deterministic order + dedup semantics.
pub(crate) fn canonicalize_value_set(values: &mut Vec<Value>) {
    if value_set_is_strictly_canonical(values.as_slice()) {
        return;
    }

    values.sort_by(Value::canonical_cmp);
    values.dedup();
}

fn value_set_is_strictly_canonical(values: &[Value]) -> bool {
    values
        .windows(2)
        .all(|pair| matches!(Value::canonical_cmp(&pair[0], &pair[1]), Ordering::Less))
}
