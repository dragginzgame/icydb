//! Module: query::plan::stability
//! Responsibility: deterministic planner stability helpers for canonical outputs.
//! Does not own: access-path selection algorithms or semantic validation.
//! Boundary: planner uses these helpers to keep equivalent query shapes stable.

use crate::{
    db::access::{AccessPlan, canonicalize_value_set, normalize_access_plan_value},
    value::Value,
};

/// Canonicalize `IN (...)` literal sets for deterministic planner lowering.
///
/// Values are sorted by canonical value ordering and deduplicated so equivalent
/// permutations lower to one stable access contract.
#[must_use]
pub(in crate::db::query::plan) fn canonicalize_in_literal_values(values: &[Value]) -> Vec<Value> {
    let mut canonical = values.to_vec();
    canonicalize_value_set(&mut canonical);
    canonical
}

/// Normalize one planner-produced access plan to the canonical stable form.
#[must_use]
pub(in crate::db::query) fn normalize_planned_access_plan_for_stability(
    plan: AccessPlan<Value>,
) -> AccessPlan<Value> {
    normalize_access_plan_value(plan)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::access::AccessPath;

    #[test]
    fn canonicalize_in_literal_values_sorts_and_deduplicates() {
        let canonical = canonicalize_in_literal_values(&[
            Value::Uint(9),
            Value::Uint(3),
            Value::Uint(9),
            Value::Uint(7),
        ]);

        assert_eq!(
            canonical,
            vec![Value::Uint(3), Value::Uint(7), Value::Uint(9)],
            "IN literal canonicalization must be order-insensitive and duplicate-insensitive",
        );
    }

    #[test]
    fn normalize_planned_access_plan_for_stability_is_idempotent() {
        let plan = AccessPlan::path(AccessPath::ByKeys(vec![
            Value::Uint(9),
            Value::Uint(3),
            Value::Uint(9),
        ]));

        let once = normalize_planned_access_plan_for_stability(plan);
        let twice = normalize_planned_access_plan_for_stability(once.clone());

        assert_eq!(
            once, twice,
            "planner stability normalization must be idempotent"
        );
    }
}
