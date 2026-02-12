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
use crate::value::{Value, ValueEnum};
use std::cmp::Ordering;

/// Canonicalize access plans that use `Value` keys.
pub(crate) fn canonicalize_access_plans_value(plans: &mut [AccessPlan<Value>]) {
    plans.sort_by(canonical_cmp_access_plan_value);
}

/// Canonicalize a list of key values for deterministic ByKeys plans.
pub(crate) fn canonicalize_key_values(keys: &mut Vec<Value>) {
    keys.sort_by(canonical_cmp_value);
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
        let cmp = canonical_cmp_value(left, right);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    left.len().cmp(&right.len())
}

/// Comparison for individual values.
///
/// Ordering rules:
/// 1. Value variant rank
/// 2. Variant-specific comparison
///
/// NOTE: Mismatched variants of the same rank must compare Equal.
/// This preserves stability without introducing semantic ordering.
/// Do NOT reuse this logic for query execution or ORDER BY.
///
fn canonical_cmp_value(left: &Value, right: &Value) -> Ordering {
    let rank = left.canonical_rank().cmp(&right.canonical_rank());
    if rank != Ordering::Equal {
        return rank;
    }

    match (left, right) {
        (Value::Account(left), Value::Account(right)) => left.cmp(right),
        (Value::Blob(left), Value::Blob(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Date(left), Value::Date(right)) => left.cmp(right),
        (Value::Decimal(left), Value::Decimal(right)) => left.cmp(right),
        (Value::Duration(left), Value::Duration(right)) => left.cmp(right),
        (Value::Enum(left), Value::Enum(right)) => canonical_cmp_value_enum(left, right),
        (Value::E8s(left), Value::E8s(right)) => left.cmp(right),
        (Value::E18s(left), Value::E18s(right)) => left.cmp(right),
        (Value::Float32(left), Value::Float32(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.cmp(right),
        (Value::Int(left), Value::Int(right)) => left.cmp(right),
        (Value::Int128(left), Value::Int128(right)) => left.cmp(right),
        (Value::IntBig(left), Value::IntBig(right)) => left.cmp(right),
        (Value::List(left), Value::List(right)) => canonical_cmp_value_list(left, right),
        (Value::Map(left), Value::Map(right)) => canonical_cmp_value_map(left, right),
        (Value::Principal(left), Value::Principal(right)) => left.cmp(right),
        (Value::Subaccount(left), Value::Subaccount(right)) => left.cmp(right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::Uint(left), Value::Uint(right)) => left.cmp(right),
        (Value::Uint128(left), Value::Uint128(right)) => left.cmp(right),
        (Value::UintBig(left), Value::UintBig(right)) => left.cmp(right),
        (Value::Ulid(left), Value::Ulid(right)) => left.cmp(right),
        _ => {
            // NOTE: Mismatched variants of the same rank compare equal by design.
            Ordering::Equal
        }
    }
}

fn canonical_cmp_value_map(left: &[(Value, Value)], right: &[(Value, Value)]) -> Ordering {
    let limit = left.len().min(right.len());
    for ((left_key, left_value), (right_key, right_value)) in
        left.iter().zip(right.iter()).take(limit)
    {
        let key_cmp = Value::canonical_cmp_key(left_key, right_key);
        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        let value_cmp = canonical_cmp_value(left_value, right_value);
        if value_cmp != Ordering::Equal {
            return value_cmp;
        }
    }

    left.len().cmp(&right.len())
}

/// Comparison for enum values.
///
/// Ordering rules:
/// 1. Variant name
/// 2. Optional path
/// 3. Optional payload
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
        (Some(left), Some(right)) => canonical_cmp_value(left, right),
    }
}
