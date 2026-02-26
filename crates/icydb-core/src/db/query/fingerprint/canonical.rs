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

use crate::{
    db::access::{AccessPath, AccessPlan},
    value::Value,
};
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
    left.canonical_cmp(right)
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

#[cfg(test)]
fn canonical_cmp_access_path_value(
    left: &AccessPath<Value>,
    right: &AccessPath<Value>,
) -> Ordering {
    left.canonical_cmp(right)
}

impl AccessPlan<Value> {
    // Compare access plans with a total deterministic ordering.
    fn canonical_cmp(&self, right: &Self) -> Ordering {
        match (self, right) {
            (Self::Path(left), Self::Path(right)) => left.canonical_cmp(right),
            (Self::Intersection(left), Self::Intersection(right))
            | (Self::Union(left), Self::Union(right)) => canonical_cmp_plan_list_value(left, right),
            _ => self.canonical_rank().cmp(&right.canonical_rank()),
        }
    }

    // Rank access-plan variants for canonical ordering.
    const fn canonical_rank(&self) -> u8 {
        match self {
            Self::Path(_) => 0,
            Self::Intersection(_) => 1,
            Self::Union(_) => 2,
        }
    }
}

impl AccessPath<Value> {
    // Compare access paths with a total deterministic ordering.
    #[expect(clippy::too_many_lines)]
    fn canonical_cmp(&self, right: &Self) -> Ordering {
        let rank = self.canonical_rank().cmp(&right.canonical_rank());
        if rank != Ordering::Equal {
            return rank;
        }

        match self {
            Self::ByKey(left_key) => {
                let Self::ByKey(right_key) = right else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
                        "canonical access path rank mismatch"
                    );
                    return Ordering::Equal;
                };
                Value::canonical_cmp(left_key, right_key)
            }
            Self::ByKeys(left_keys) => {
                let Self::ByKeys(right_keys) = right else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
                        "canonical access path rank mismatch"
                    );
                    return Ordering::Equal;
                };
                canonical_cmp_value_list(left_keys, right_keys)
            }
            Self::KeyRange {
                start: left_start,
                end: left_end,
            } => {
                let Self::KeyRange {
                    start: right_start,
                    end: right_end,
                } = right
                else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
                        "canonical access path rank mismatch"
                    );
                    return Ordering::Equal;
                };

                let cmp = Value::canonical_cmp(left_start, right_start);
                if cmp != Ordering::Equal {
                    return cmp;
                }
                Value::canonical_cmp(left_end, right_end)
            }
            Self::IndexPrefix {
                index: left_index,
                values: left_values,
            } => {
                let Self::IndexPrefix {
                    index: right_index,
                    values: right_values,
                } = right
                else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
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
            Self::IndexRange { spec: left_spec } => {
                let Self::IndexRange { spec: right_spec } = right else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
                        "canonical access path rank mismatch"
                    );
                    return Ordering::Equal;
                };

                let cmp = left_spec.index().name.cmp(right_spec.index().name);
                if cmp != Ordering::Equal {
                    return cmp;
                }

                let cmp = left_spec.index().fields.cmp(right_spec.index().fields);
                if cmp != Ordering::Equal {
                    return cmp;
                }

                let cmp = left_spec
                    .prefix_values()
                    .len()
                    .cmp(&right_spec.prefix_values().len());
                if cmp != Ordering::Equal {
                    return cmp;
                }

                let cmp =
                    canonical_cmp_value_list(left_spec.prefix_values(), right_spec.prefix_values());
                if cmp != Ordering::Equal {
                    return cmp;
                }

                let cmp = canonical_cmp_value_bound(left_spec.lower(), right_spec.lower());
                if cmp != Ordering::Equal {
                    return cmp;
                }

                canonical_cmp_value_bound(left_spec.upper(), right_spec.upper())
            }
            Self::FullScan => {
                let Self::FullScan = right else {
                    debug_assert_eq!(
                        self.canonical_rank(),
                        right.canonical_rank(),
                        "canonical access path rank mismatch"
                    );
                    return Ordering::Equal;
                };
                Ordering::Equal
            }
        }
    }

    // Rank access-path variants for canonical ordering.
    const fn canonical_rank(&self) -> AccessPathRank {
        match self {
            Self::ByKey(_) => AccessPathRank { tier: 0, detail: 0 },
            Self::ByKeys(_) => AccessPathRank { tier: 0, detail: 1 },
            Self::KeyRange { .. } => AccessPathRank { tier: 0, detail: 2 },
            Self::IndexRange { .. } => AccessPathRank { tier: 1, detail: 0 },
            Self::IndexPrefix { index, values } => AccessPathRank {
                tier: 1,
                detail: if values.len() == index.fields.len() {
                    1
                } else {
                    2
                },
            },
            Self::FullScan => AccessPathRank { tier: 2, detail: 0 },
        }
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

fn canonical_cmp_value_bound(left: &Bound<Value>, right: &Bound<Value>) -> Ordering {
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => Value::canonical_cmp(left, right),
        (Bound::Included(left), Bound::Excluded(right)) => {
            let cmp = Value::canonical_cmp(left, right);
            if cmp == Ordering::Equal {
                Ordering::Less
            } else {
                cmp
            }
        }
        (Bound::Excluded(left), Bound::Included(right)) => {
            let cmp = Value::canonical_cmp(left, right);
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
        db::query::{ReadConsistency, plan::AccessPlannedQuery},
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
        AccessPath::index_range(TEST_INDEX, vec![Value::Uint(7)], lower, upper)
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

        let included_plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(included, ReadConsistency::MissingOk);
        let excluded_plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(excluded, ReadConsistency::MissingOk);
        assert_ne!(included_plan.fingerprint(), excluded_plan.fingerprint());
    }

    #[test]
    fn canonical_and_fingerprint_align_for_index_field_identity() {
        let path_a = AccessPath::index_range(
            TEST_INDEX,
            vec![Value::Uint(7)],
            Bound::Included(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );
        let path_b = AccessPath::index_range(
            TEST_INDEX_SAME_NAME_ALT_FIELDS,
            vec![Value::Uint(7)],
            Bound::Included(Value::Uint(100)),
            Bound::Excluded(Value::Uint(200)),
        );

        assert_ne!(
            canonical_cmp_access_path_value(&path_a, &path_b),
            Ordering::Equal
        );

        let plan_a: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(path_a, ReadConsistency::MissingOk);
        let plan_b: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(path_b, ReadConsistency::MissingOk);
        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }
}
