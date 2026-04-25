//! Module: value::canonical
//! Responsibility: deterministic canonical ordering helpers for dynamic values.
//! Does not own: predicate normalization, access planning, or storage layout.
//! Boundary: shared value-level canonicalization used by higher db layers.

use crate::value::Value;

/// Canonicalize one value set with deterministic order + dedup semantics.
pub(crate) fn canonicalize_value_set(values: &mut Vec<Value>) {
    values.sort_by(Value::canonical_cmp);
    values.dedup();
}
