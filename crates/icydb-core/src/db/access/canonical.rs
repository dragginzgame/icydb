//! Module: access::canonical
//! Responsibility: deterministic access-shape canonicalization.
//! Does not own: query semantics or schema-aware ordering rules.
//! Boundary: canonicalization is used for deterministic planning/fingerprinting.
//!
//! Canonicalization invariants:
//! - canonicalization must be idempotent
//! - canonical ordering must be total
//! - semantically equivalent access shapes must normalize to identical plans
//! - canonical shape must align with query fingerprinting
//!
//! Any change in this module must preserve fingerprint/continuation stability.

use crate::{
    db::access::{AccessPath, AccessPlan, SemanticIndexRangeSpec},
    model::index::IndexModel,
    value::Value,
};
use std::cmp::Ordering;
use std::ops::Bound;

/// Canonicalize access plans that use `Value` keys.
fn canonicalize_access_plans_value(plans: &mut [AccessPlan<Value>]) {
    // Canonical sort is total and must remain deterministic.
    plans.sort_by(canonical_cmp_access_plan_value);
}

// Deduplicate already-canonicalized access plans without retaining the generic
// `Vec::dedup` / `PartialEq` path for this domain-specific ordering contract.
fn dedup_sorted_access_plans(plans: &mut Vec<AccessPlan<Value>>) {
    if plans.len() < 2 {
        return;
    }

    let mut write = 1usize;
    for read in 1..plans.len() {
        if canonical_cmp_access_plan_value(&plans[write - 1], &plans[read]) == Ordering::Equal {
            continue;
        }

        if write != read {
            plans.swap(write, read);
        }
        write += 1;
    }

    plans.truncate(write);
}

/// Canonicalize one value set with deterministic order + dedup semantics.
pub(in crate::db) fn canonicalize_value_set(values: &mut Vec<Value>) {
    values.sort_by(Value::canonical_cmp);
    values.dedup();
}

/// Normalize one value-keyed access plan into deterministic canonical shape.
#[must_use]
pub(crate) fn normalize_access_plan_value(plan: AccessPlan<Value>) -> AccessPlan<Value> {
    plan.normalize_for_access()
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
    for (left, right) in left.iter().zip(right.iter()) {
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

// Return the single value from one canonicalized value-set shape.
fn single_canonical_value(values: &[Value]) -> Option<&Value> {
    match values {
        [value] => Some(value),
        _ => None,
    }
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

    // Normalize this access plan into a canonical deterministic form.
    fn normalize_for_access(self) -> Self {
        match self {
            Self::Path(path) => Self::path(path.normalize_for_access()),
            Self::Union(children) => Self::normalize_union(children),
            Self::Intersection(children) => Self::normalize_intersection(children),
        }
    }

    fn normalize_union(children: Vec<Self>) -> Self {
        let mut out = Vec::new();
        let mut saw_explicit_empty = false;

        for child in children {
            let child = child.normalize_for_access();
            if child.is_single_full_scan() {
                return Self::full_scan();
            }
            if child.is_explicit_empty() {
                saw_explicit_empty = true;
                continue;
            }

            Self::append_union_child(&mut out, child);
        }
        if out.is_empty() && saw_explicit_empty {
            return Self::path(AccessPath::ByKeys(Vec::new()));
        }

        Self::collapse_normalized_composite(out, true)
    }

    fn normalize_intersection(children: Vec<Self>) -> Self {
        let mut out = Vec::new();

        for child in children {
            let child = child.normalize_for_access();
            if child.is_single_full_scan() {
                continue;
            }
            if child.is_explicit_empty() {
                return child;
            }

            Self::append_intersection_child(&mut out, child);
        }
        if let Some(empty_child) = out.iter().position(Self::is_explicit_empty) {
            return out.remove(empty_child);
        }

        Self::collapse_normalized_composite(out, false)
    }

    fn collapse_normalized_composite(mut out: Vec<Self>, is_union: bool) -> Self {
        if out.is_empty() {
            return Self::full_scan();
        }
        if out.len() == 1 {
            return out.pop().expect("single composite child");
        }

        canonicalize_access_plans_value(&mut out);
        dedup_sorted_access_plans(&mut out);
        if out.len() == 1 {
            return out.pop().expect("single composite child");
        }

        if is_union {
            Self::Union(out)
        } else {
            Self::Intersection(out)
        }
    }

    fn append_union_child(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Union(children) => out.extend(children),
            other => out.push(other),
        }
    }

    fn append_intersection_child(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Intersection(children) => out.extend(children),
            other => out.push(other),
        }
    }
}

impl AccessPath<Value> {
    // Normalize one concrete access path for deterministic planning.
    fn normalize_for_access(self) -> Self {
        match self {
            Self::ByKeys(mut keys) => {
                canonicalize_value_set(&mut keys);
                if let Some(key) = single_canonical_value(keys.as_slice()) {
                    return Self::ByKey(key.clone());
                }

                Self::ByKeys(keys)
            }
            Self::IndexMultiLookup { index, mut values } => {
                canonicalize_value_set(&mut values);
                if let Some(value) = single_canonical_value(values.as_slice()) {
                    return Self::IndexPrefix {
                        index,
                        values: vec![value.clone()],
                    };
                }

                Self::IndexMultiLookup { index, values }
            }
            other => other,
        }
    }

    // Compare access paths with a total deterministic ordering.
    fn canonical_cmp(&self, right: &Self) -> Ordering {
        let rank = self.canonical_rank().cmp(&right.canonical_rank());
        if rank != Ordering::Equal {
            return rank;
        }

        // Once the canonical rank matches, only one same-rank variant pairing is valid.
        match (self, right) {
            (Self::ByKey(left_key), Self::ByKey(right_key)) => {
                Value::canonical_cmp(left_key, right_key)
            }
            (Self::ByKeys(left_keys), Self::ByKeys(right_keys)) => {
                canonical_cmp_value_list(left_keys, right_keys)
            }
            (
                Self::KeyRange {
                    start: left_start,
                    end: left_end,
                },
                Self::KeyRange {
                    start: right_start,
                    end: right_end,
                },
            ) => Self::canonical_cmp_key_range(left_start, left_end, right_start, right_end),
            (
                Self::IndexPrefix {
                    index: left_index,
                    values: left_values,
                },
                Self::IndexPrefix {
                    index: right_index,
                    values: right_values,
                },
            ) => {
                Self::canonical_cmp_index_prefix(left_index, left_values, right_index, right_values)
            }
            (
                Self::IndexMultiLookup {
                    index: left_index,
                    values: left_values,
                },
                Self::IndexMultiLookup {
                    index: right_index,
                    values: right_values,
                },
            ) => Self::canonical_cmp_index_multi_lookup(
                left_index,
                left_values,
                right_index,
                right_values,
            ),
            (Self::IndexRange { spec: left_spec }, Self::IndexRange { spec: right_spec }) => {
                Self::canonical_cmp_index_range(left_spec, right_spec)
            }
            (Self::FullScan, Self::FullScan) => Ordering::Equal,
            _ => canonical_cmp_access_path_rank_mismatch(self, right),
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
                detail: if values.len() == index.fields().len() {
                    1
                } else {
                    2
                },
            },
            Self::IndexMultiLookup { .. } => AccessPathRank { tier: 1, detail: 3 },
            Self::FullScan => AccessPathRank { tier: 2, detail: 0 },
        }
    }

    // Compare key-range bounds once the variant pairing is already fixed.
    fn canonical_cmp_key_range(
        left_start: &Value,
        left_end: &Value,
        right_start: &Value,
        right_end: &Value,
    ) -> Ordering {
        let cmp = Value::canonical_cmp(left_start, right_start);
        if cmp != Ordering::Equal {
            return cmp;
        }

        Value::canonical_cmp(left_end, right_end)
    }

    // Compare one index-prefix shape after rank + variant pairing succeeds.
    fn canonical_cmp_index_prefix(
        left_index: &IndexModel,
        left_values: &[Value],
        right_index: &IndexModel,
        right_values: &[Value],
    ) -> Ordering {
        let cmp = canonical_cmp_index_identity(*left_index, *right_index);
        if cmp != Ordering::Equal {
            return cmp;
        }

        let cmp = left_values.len().cmp(&right_values.len());
        if cmp != Ordering::Equal {
            return cmp;
        }

        canonical_cmp_value_list(left_values, right_values)
    }

    // Compare one index multi-lookup shape after rank + variant pairing succeeds.
    fn canonical_cmp_index_multi_lookup(
        left_index: &IndexModel,
        left_values: &[Value],
        right_index: &IndexModel,
        right_values: &[Value],
    ) -> Ordering {
        let cmp = canonical_cmp_index_identity(*left_index, *right_index);
        if cmp != Ordering::Equal {
            return cmp;
        }

        canonical_cmp_value_list(left_values, right_values)
    }

    // Compare one semantic index-range shape after rank + variant pairing succeeds.
    fn canonical_cmp_index_range(
        left_spec: &SemanticIndexRangeSpec,
        right_spec: &SemanticIndexRangeSpec,
    ) -> Ordering {
        let cmp = canonical_cmp_index_identity(*left_spec.index(), *right_spec.index());
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

        let cmp = canonical_cmp_value_list(left_spec.prefix_values(), right_spec.prefix_values());
        if cmp != Ordering::Equal {
            return cmp;
        }

        let cmp = canonical_cmp_value_bound(left_spec.lower(), right_spec.lower());
        if cmp != Ordering::Equal {
            return cmp;
        }

        canonical_cmp_value_bound(left_spec.upper(), right_spec.upper())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AccessPathRank {
    tier: u8,
    detail: u8,
}

// Guard against impossible same-rank mismatches without duplicating assertions per variant.
fn canonical_cmp_access_path_rank_mismatch(
    left: &AccessPath<Value>,
    right: &AccessPath<Value>,
) -> Ordering {
    debug_assert_eq!(
        left.canonical_rank(),
        right.canonical_rank(),
        "canonical access path rank mismatch"
    );

    Ordering::Equal
}

// Compare index identity in canonical order before considering payload values.
fn canonical_cmp_index_identity(left: IndexModel, right: IndexModel) -> Ordering {
    let cmp = left.name().cmp(right.name());
    if cmp != Ordering::Equal {
        return cmp;
    }

    left.fields().cmp(right.fields())
}

/// Lexicographic comparison of value lists.
fn canonical_cmp_value_list(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
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
    use crate::{model::index::IndexModel, types::Ulid};

    const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const TEST_INDEX: IndexModel = IndexModel::generated(
        "canonical::group_rank",
        "canonical::store",
        &TEST_INDEX_FIELDS,
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
    fn normalize_by_keys_singleton_collapses_to_by_key() {
        let key = Value::Ulid(Ulid::from_u128(7));
        let normalized =
            normalize_access_plan_value(AccessPlan::path(AccessPath::ByKeys(vec![key.clone()])));

        assert_eq!(normalized, AccessPlan::path(AccessPath::ByKey(key)));
    }

    #[test]
    fn normalize_index_multi_lookup_singleton_collapses_to_index_prefix() {
        let normalized =
            normalize_access_plan_value(AccessPlan::path(AccessPath::IndexMultiLookup {
                index: TEST_INDEX,
                values: vec![Value::Uint(7)],
            }));

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::IndexPrefix {
                index: TEST_INDEX,
                values: vec![Value::Uint(7)],
            }),
        );
    }

    #[test]
    fn normalize_index_multi_lookup_canonicalizes_value_set() {
        let normalized =
            normalize_access_plan_value(AccessPlan::path(AccessPath::IndexMultiLookup {
                index: TEST_INDEX,
                values: vec![Value::Uint(9), Value::Uint(7), Value::Uint(9)],
            }));

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::IndexMultiLookup {
                index: TEST_INDEX,
                values: vec![Value::Uint(7), Value::Uint(9)],
            }),
        );
    }

    #[test]
    fn normalize_access_plan_value_is_idempotent() {
        let k1 = Value::Ulid(Ulid::from_u128(1));
        let k2 = Value::Ulid(Ulid::from_u128(2));
        let raw = AccessPlan::intersection(vec![
            AccessPlan::union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![k2, k1.clone(), k1.clone()])),
                AccessPlan::path(AccessPath::ByKeys(vec![k1])),
            ]),
            AccessPlan::full_scan(),
        ]);

        let once = normalize_access_plan_value(raw);
        let twice = normalize_access_plan_value(once.clone());

        assert_eq!(once, twice, "access canonicalization must be idempotent");
    }

    #[test]
    fn normalize_intersection_with_explicit_empty_collapses_to_empty() {
        let normalized = normalize_access_plan_value(AccessPlan::intersection(vec![
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7)))),
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
            AccessPlan::full_scan(),
        ]));

        assert_eq!(normalized, AccessPlan::path(AccessPath::ByKeys(Vec::new())));
    }

    #[test]
    fn normalize_union_with_explicit_empty_collapses_to_non_empty_branch() {
        let normalized = normalize_access_plan_value(AccessPlan::union(vec![
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7)))),
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        ]));

        assert_eq!(
            normalized,
            AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::from_u128(7))))
        );
    }

    #[test]
    fn normalize_union_only_explicit_empty_children_stays_empty() {
        let normalized = normalize_access_plan_value(AccessPlan::union(vec![
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
            AccessPlan::path(AccessPath::ByKeys(Vec::new())),
        ]));

        assert_eq!(normalized, AccessPlan::path(AccessPath::ByKeys(Vec::new())));
    }
}
