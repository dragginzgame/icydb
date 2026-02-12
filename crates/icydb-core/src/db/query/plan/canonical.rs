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

    match (left, right) {
        (AccessPath::ByKey(left), AccessPath::ByKey(right)) => canonical_cmp_value(left, right),

        (AccessPath::ByKeys(left), AccessPath::ByKeys(right)) => {
            canonical_cmp_value_list(left, right)
        }

        (
            AccessPath::KeyRange {
                start: left_start,
                end: left_end,
            },
            AccessPath::KeyRange {
                start: right_start,
                end: right_end,
            },
        ) => {
            let cmp = canonical_cmp_value(left_start, right_start);
            if cmp != Ordering::Equal {
                return cmp;
            }
            canonical_cmp_value(left_end, right_end)
        }

        (
            AccessPath::IndexPrefix {
                index: left_index,
                values: left_values,
            },
            AccessPath::IndexPrefix {
                index: right_index,
                values: right_values,
            },
        ) => {
            let cmp = left_index.name.cmp(right_index.name);
            if cmp != Ordering::Equal {
                return cmp;
            }

            let cmp = left_values.len().cmp(&right_values.len());
            if cmp != Ordering::Equal {
                return cmp;
            }

            canonical_cmp_value_list(left_values, right_values)
        }

        _ => {
            debug_assert_eq!(
                canonical_access_path_rank(left),
                canonical_access_path_rank(right),
                "canonical access path rank mismatch"
            );
            // NOTE: Rank ties are treated as equal to preserve deterministic ordering.
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
        AccessPath::IndexPrefix { index, values } => AccessPathRank {
            tier: 1,
            detail: if values.len() == index.fields.len() {
                0
            } else {
                1
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
