//! Deterministic ordering for planner output.
//!
//! This module is responsible **only** for making planner results stable and
//! reproducible. It must never encode, infer, or enforce query semantics.
//!
//! Invariants:
//! - Ordering here must be total and deterministic.
//! - Ordering must not depend on runtime state or schema knowledge.
//! - Changing order here must never change query meaning.
//!
//! If an ordering decision appears to imply semantic preference, it does not
//! belong in this module.

use super::types::{AccessPath, AccessPlan};
use crate::value::Value;
use std::cmp::Ordering;
use std::ops::Bound;

/// Canonicalize access plans that use `Value` keys.
pub(crate) fn canonicalize_access_plans_value(plans: &mut [AccessPlan<Value>]) {
    plans.sort_by(canonical_cmp_access_plan_value);
}

/// Canonicalize a list of key values for deterministic ByKeys plans.
pub(crate) fn canonicalize_key_values(keys: &mut Vec<Value>) {
    keys.sort_by(Value::canonical_cmp);
    keys.dedup();
}

fn canonical_cmp_access_plan_value(
    left: &AccessPlan<Value>,
    right: &AccessPlan<Value>,
) -> Ordering {
    match (left, right) {
        (AccessPlan::Path(left), AccessPlan::Path(right)) => {
            canonical_cmp_access_path_value(left, right)
        }
        (AccessPlan::Intersection(left), AccessPlan::Intersection(right))
        | (AccessPlan::Union(left), AccessPlan::Union(right)) => {
            canonical_cmp_plan_list_value(left, right)
        }
        _ => canonical_access_plan_rank(left).cmp(&canonical_access_plan_rank(right)),
    }
}

/// Assigns a total ordering across access plan variants.
///
/// Lower values sort first.
const fn canonical_access_plan_rank<K>(plan: &AccessPlan<K>) -> u8 {
    match plan {
        AccessPlan::Path(_) => 0,
        AccessPlan::Intersection(_) => 1,
        AccessPlan::Union(_) => 2,
    }
}

fn canonical_cmp_plan_list_value(
    left: &[AccessPlan<Value>],
    right: &[AccessPlan<Value>],
) -> Ordering {
    let limit = left.len().min(right.len());
    for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
        let cmp = canonical_cmp_access_plan_value(left, right);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    left.len().cmp(&right.len())
}

fn canonical_cmp_access_path_value(
    left: &AccessPath<Value>,
    right: &AccessPath<Value>,
) -> Ordering {
    let rank = canonical_access_path_rank(left).cmp(&canonical_access_path_rank(right));
    if rank != Ordering::Equal {
        return rank;
    }

    match left {
        AccessPath::ByKey(left_key) => {
            let AccessPath::ByKey(right_key) = right else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };
            canonical_cmp_value(left_key, right_key)
        }
        AccessPath::ByKeys(left_keys) => {
            let AccessPath::ByKeys(right_keys) = right else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };
            canonical_cmp_value_list(left_keys, right_keys)
        }
        AccessPath::KeyRange {
            start: left_start,
            end: left_end,
        } => {
            let AccessPath::KeyRange {
                start: right_start,
                end: right_end,
            } = right
            else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };

            let cmp = canonical_cmp_value(left_start, right_start);
            if cmp != Ordering::Equal {
                return cmp;
            }
            canonical_cmp_value(left_end, right_end)
        }
        AccessPath::IndexPrefix {
            index: left_index,
            values: left_values,
        } => {
            let AccessPath::IndexPrefix {
                index: right_index,
                values: right_values,
            } = right
            else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };

            let cmp = left_index.name.cmp(right_index.name);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = left_index.fields.cmp(right_index.fields);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = left_values.len().cmp(&right_values.len());
            if cmp != Ordering::Equal {
                return cmp;
            }

            canonical_cmp_value_list(left_values, right_values)
        }
        AccessPath::IndexRange {
            index: left_index,
            prefix: left_prefix,
            lower: left_lower,
            upper: left_upper,
        } => {
            let AccessPath::IndexRange {
                index: right_index,
                prefix: right_prefix,
                lower: right_lower,
                upper: right_upper,
            } = right
            else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };

            let cmp = left_index.name.cmp(right_index.name);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = left_index.fields.cmp(right_index.fields);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = left_prefix.len().cmp(&right_prefix.len());
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = canonical_cmp_value_list(left_prefix, right_prefix);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = canonical_cmp_value_bound(left_lower, right_lower);
            if cmp != Ordering::Equal {
                return cmp;
            }

            canonical_cmp_value_bound(left_upper, right_upper)
        }
        AccessPath::FullScan => {
            let AccessPath::FullScan = right else {
                debug_assert_eq!(
                    canonical_access_path_rank(left),
                    canonical_access_path_rank(right),
                    "canonical access path rank mismatch"
                );
                return Ordering::Equal;
            };
            Ordering::Equal
        }
    }
}

/// Assigns a total ordering across access path variants.
///
/// Lower values sort first.
const fn canonical_access_path_rank<K>(path: &AccessPath<K>) -> AccessPathRank {
    match path {
        AccessPath::ByKey(_) => AccessPathRank { tier: 0, detail: 0 },
        AccessPath::ByKeys(_) => AccessPathRank { tier: 0, detail: 1 },
        AccessPath::KeyRange { .. } => AccessPathRank { tier: 0, detail: 2 },
        AccessPath::IndexRange { .. } => AccessPathRank { tier: 1, detail: 0 },
        AccessPath::IndexPrefix { index, values } => AccessPathRank {
            tier: 1,
            detail: if values.len() == index.fields.len() {
                1
            } else {
                2
            },
        },
        AccessPath::FullScan => AccessPathRank { tier: 2, detail: 0 },
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AccessPathRank {
    tier: u8,
    detail: u8,
}

/// Lexicographic comparison of value lists.
fn canonical_cmp_value_list(left: &[Value], right: &[Value]) -> Ordering {
    let limit = left.len().min(right.len());
    for (left, right) in left.iter().take(limit).zip(right.iter().take(limit)) {
        let cmp = Value::canonical_cmp(left, right);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    left.len().cmp(&right.len())
}

fn canonical_cmp_value(left: &Value, right: &Value) -> Ordering {
    Value::canonical_cmp(left, right)
}

fn canonical_cmp_value_bound(left: &Bound<Value>, right: &Bound<Value>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => canonical_cmp_value(left, right),
        (Bound::Included(left), Bound::Excluded(right)) => {
            let cmp = canonical_cmp_value(left, right);
            if cmp == Ordering::Equal {
                Ordering::Less
            } else {
                cmp
            }
        }
        (Bound::Excluded(left), Bound::Included(right)) => {
            let cmp = canonical_cmp_value(left, right);
            if cmp == Ordering::Equal {
                Ordering::Greater
            } else {
                cmp
            }
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::{ReadConsistency, plan::LogicalPlan},
        model::index::IndexModel,
    };

    const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const TEST_INDEX: IndexModel = IndexModel::new(
        "canonical::group_rank",
        "canonical::store",
        &TEST_INDEX_FIELDS,
        false,
    );
    const TEST_INDEX_FIELDS_ALT: [&str; 2] = ["group", "score"];
    const TEST_INDEX_SAME_NAME_ALT_FIELDS: IndexModel = IndexModel::new(
        "canonical::group_rank",
        "canonical::store",
        &TEST_INDEX_FIELDS_ALT,
        false,
    );

    fn index_range_path(lower: Bound<Value>, upper: Bound<Value>) -> AccessPath<Value> {
        AccessPath::IndexRange {
            index: TEST_INDEX,
            prefix: vec![Value::Uint(7)],
            lower,
            upper,
        }
    }

    #[test]
    fn canonical_bound_ordering_is_unbounded_then_included_then_excluded() {
        let value = Value::Uint(100);

        assert_eq!(
            canonical_cmp_value_bound(&Bound::Unbounded, &Bound::Included(value.clone())),
            Ordering::Less
        );
        assert_eq!(
            canonical_cmp_value_bound(&Bound::Included(value.clone()), &Bound::Unbounded),
            Ordering::Greater
        );
        assert_eq!(
            canonical_cmp_value_bound(
                &Bound::Included(value.clone()),
                &Bound::Excluded(value.clone()),
            ),
            Ordering::Less
        );
        assert_eq!(
            canonical_cmp_value_bound(&Bound::Excluded(value.clone()), &Bound::Included(value)),
            Ordering::Greater
        );
    }

    #[test]
    fn canonical_index_range_cmp_distinguishes_bound_discriminants() {
        let included = index_range_path(
            Bound::Included(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );
        let excluded = index_range_path(
            Bound::Excluded(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );

        assert_eq!(
            canonical_cmp_access_path_value(&included, &excluded),
            Ordering::Less
        );
        assert_eq!(
            canonical_cmp_access_path_value(&excluded, &included),
            Ordering::Greater
        );
    }

    #[test]
    fn canonical_and_fingerprint_align_for_index_range_bound_discriminants() {
        let included = index_range_path(
            Bound::Included(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );
        let excluded = index_range_path(
            Bound::Excluded(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );

        assert_ne!(
            canonical_cmp_access_path_value(&included, &excluded),
            Ordering::Equal
        );

        let included_plan: LogicalPlan<Value> =
            LogicalPlan::new(included, ReadConsistency::MissingOk);
        let excluded_plan: LogicalPlan<Value> =
            LogicalPlan::new(excluded, ReadConsistency::MissingOk);
        assert_ne!(included_plan.fingerprint(), excluded_plan.fingerprint());
    }

    #[test]
    fn canonical_and_fingerprint_align_for_index_field_identity() {
        let path_a = AccessPath::IndexRange {
            index: TEST_INDEX,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(100)),
            upper: Bound::Excluded(Value::Uint(200)),
        };
        let path_b = AccessPath::IndexRange {
            index: TEST_INDEX_SAME_NAME_ALT_FIELDS,
            prefix: vec![Value::Uint(7)],
            lower: Bound::Included(Value::Uint(100)),
            upper: Bound::Excluded(Value::Uint(200)),
        };

        assert_ne!(
            canonical_cmp_access_path_value(&path_a, &path_b),
            Ordering::Equal
        );

        let plan_a: LogicalPlan<Value> = LogicalPlan::new(path_a, ReadConsistency::MissingOk);
        let plan_b: LogicalPlan<Value> = LogicalPlan::new(path_b, ReadConsistency::MissingOk);
        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }
}
