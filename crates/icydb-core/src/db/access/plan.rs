//! Module: access::plan
//! Responsibility: composite access-plan structure and pushdown eligibility modeling.
//! Does not own: schema validation or raw-bound lowering.
//! Boundary: query planner emits these plans for executor routing.

use crate::{
    db::access::{AccessPath, AccessStrategy, IndexRangePathRef, SemanticIndexRangeSpec},
    model::index::IndexModel,
    traits::KeyValueCodec,
    value::Value,
};

///
/// AccessPlan
/// Composite access structure; may include unions/intersections and is runtime-resolvable.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccessPlan<K> {
    Path(Box<AccessPath<K>>),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

impl<K> AccessPlan<K> {
    /// Construct a plan from one concrete access path.
    #[must_use]
    pub(crate) fn path(path: AccessPath<K>) -> Self {
        Self::Path(Box::new(path))
    }

    /// Construct a single-key access plan.
    #[must_use]
    pub(crate) fn by_key(key: K) -> Self {
        Self::path(AccessPath::ByKey(key))
    }

    /// Construct a multi-key access plan.
    #[must_use]
    pub(crate) fn by_keys(keys: Vec<K>) -> Self {
        Self::path(AccessPath::ByKeys(keys))
    }

    /// Construct a primary-key range access plan.
    #[must_use]
    pub(crate) fn key_range(start: K, end: K) -> Self {
        Self::path(AccessPath::KeyRange { start, end })
    }

    /// Construct an index-prefix access plan.
    #[must_use]
    pub(crate) fn index_prefix(index: IndexModel, values: Vec<Value>) -> Self {
        Self::path(AccessPath::IndexPrefix { index, values })
    }

    /// Construct an index multi-lookup access plan.
    #[must_use]
    pub(crate) fn index_multi_lookup(index: IndexModel, values: Vec<Value>) -> Self {
        Self::path(AccessPath::IndexMultiLookup { index, values })
    }

    /// Construct an index-range access plan from one semantic range descriptor.
    #[must_use]
    pub(crate) fn index_range(spec: SemanticIndexRangeSpec) -> Self {
        Self::path(AccessPath::IndexRange { spec })
    }

    /// Construct a plan that forces a full scan.
    #[must_use]
    pub(crate) fn full_scan() -> Self {
        Self::path(AccessPath::FullScan)
    }

    /// Construct a canonical union access plan.
    ///
    /// Canonicalization guarantees:
    /// - nested union nodes are flattened
    /// - empty unions collapse to a full scan identity node
    /// - single-child unions collapse to that child
    #[must_use]
    pub(crate) fn union(children: Vec<Self>) -> Self {
        let mut out = Vec::new();
        let mut saw_explicit_empty = false;
        for child in children {
            Self::append_flattened_child(&mut out, child, true);
        }
        out.retain(|child| {
            let is_empty = child.is_explicit_empty();
            if is_empty {
                saw_explicit_empty = true;
            }

            !is_empty
        });
        if out.is_empty() && saw_explicit_empty {
            return Self::by_keys(Vec::new());
        }

        Self::collapse_canonical_composite(out, true)
    }

    /// Construct a canonical intersection access plan.
    ///
    /// Canonicalization guarantees:
    /// - nested intersection nodes are flattened
    /// - empty intersections collapse to a full scan identity node
    /// - single-child intersections collapse to that child
    #[must_use]
    pub(crate) fn intersection(children: Vec<Self>) -> Self {
        let mut out = Vec::new();
        for child in children {
            Self::append_flattened_child(&mut out, child, false);
        }
        if let Some(empty_child) = out.iter().position(Self::is_explicit_empty) {
            return out.remove(empty_child);
        }

        Self::collapse_canonical_composite(out, false)
    }

    /// Borrow the concrete path when this plan is a single-path node.
    #[must_use]
    pub(crate) fn as_path(&self) -> Option<&AccessPath<K>> {
        match self {
            Self::Path(path) => Some(path.as_ref()),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }

    /// Return true when this plan is exactly one full-scan path.
    #[must_use]
    pub(crate) const fn is_single_full_scan(&self) -> bool {
        matches!(self, Self::Path(path) if path.is_full_scan())
    }

    /// Return true when this plan is exactly one explicit empty key set.
    #[must_use]
    pub(crate) fn is_explicit_empty(&self) -> bool {
        matches!(self, Self::Path(path) if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty()))
    }

    /// Return true when this plan is one singleton primary-key lookup shape.
    #[must_use]
    pub(crate) fn is_singleton_or_empty_primary_key_access(&self) -> bool {
        let Some(path) = self.as_path() else {
            return false;
        };

        path.as_by_key().is_some() || path.as_by_keys().is_some_and(|keys| keys.len() <= 1)
    }

    /// Borrow index-prefix access details when this is a single IndexPrefix path.
    #[must_use]
    pub(crate) fn as_index_prefix_path(&self) -> Option<(&IndexModel, &[Value])> {
        self.as_path().and_then(|path| path.as_index_prefix())
    }

    /// Borrow index-range access details when this is a single IndexRange path.
    #[must_use]
    pub(crate) fn as_index_range_path(&self) -> Option<IndexRangePathRef<'_>> {
        self.as_path().and_then(|path| path.as_index_range())
    }

    /// Borrow the primary-key range endpoints when this is a single `KeyRange`
    /// path.
    #[must_use]
    pub(crate) fn as_primary_key_range_path(&self) -> Option<(&K, &K)> {
        self.as_path().and_then(|path| path.as_key_range())
    }

    /// Borrow the primary-key payload when this is a single `ByKey` path.
    #[must_use]
    pub(crate) fn as_by_key_path(&self) -> Option<&K> {
        self.as_path().and_then(|path| path.as_by_key())
    }

    /// Borrow the selected secondary index model when this is a single
    /// secondary-index access path.
    #[must_use]
    pub(crate) fn selected_index_model(&self) -> Option<&IndexModel> {
        self.as_path().and_then(|path| path.selected_index_model())
    }

    /// Resolve one pre-lowered access strategy contract for runtime execution.
    #[must_use]
    pub(in crate::db) fn resolve_strategy(&self) -> AccessStrategy<'_, K> {
        AccessStrategy::from_plan(self)
    }

    /// Map key payloads across this access tree while preserving structural shape.
    pub(crate) fn map_keys<T, E, F>(self, mut map_key: F) -> Result<AccessPlan<T>, E>
    where
        F: FnMut(K) -> Result<T, E>,
    {
        self.map_keys_with(&mut map_key)
    }

    // Collapse an already-flattened composite node into canonical arity form.
    fn collapse_canonical_composite(mut children: Vec<Self>, is_union: bool) -> Self {
        if children.is_empty() {
            return Self::full_scan();
        }
        if children.len() == 1 {
            return children.pop().expect("single composite child");
        }

        if is_union {
            Self::Union(children)
        } else {
            Self::Intersection(children)
        }
    }

    // Append one child into the requested flattened composite accumulator.
    fn append_flattened_child(out: &mut Vec<Self>, child: Self, flatten_union: bool) {
        match child {
            Self::Union(children) if flatten_union => {
                for child in children {
                    Self::append_flattened_child(out, child, flatten_union);
                }
            }
            Self::Intersection(children) if !flatten_union => {
                for child in children {
                    Self::append_flattened_child(out, child, flatten_union);
                }
            }
            other => out.push(other),
        }
    }

    // Shared recursive mapper so one mutable key-mapping closure can be reused.
    fn map_keys_with<T, E, F>(self, map_key: &mut F) -> Result<AccessPlan<T>, E>
    where
        F: FnMut(K) -> Result<T, E>,
    {
        match self {
            Self::Path(path) => Ok(AccessPlan::path(path.map_keys(map_key)?)),
            Self::Union(children) => {
                Ok(AccessPlan::union(Self::map_child_plans(children, map_key)?))
            }
            Self::Intersection(children) => Ok(AccessPlan::intersection(Self::map_child_plans(
                children, map_key,
            )?)),
        }
    }

    // Map one child-plan list with one shared mutable key-mapping closure.
    fn map_child_plans<T, E, F>(
        children: Vec<Self>,
        map_key: &mut F,
    ) -> Result<Vec<AccessPlan<T>>, E>
    where
        F: FnMut(K) -> Result<T, E>,
    {
        let mut out = Vec::with_capacity(children.len());
        for child in children {
            out.push(child.map_keys_with(map_key)?);
        }

        Ok(out)
    }
}

impl<K> AccessPlan<K>
where
    K: KeyValueCodec,
{
    /// Convert one typed access plan into the canonical structural `Value` form.
    #[must_use]
    pub(crate) fn into_value_plan(self) -> AccessPlan<Value> {
        self.map_keys(|key| Ok::<Value, core::convert::Infallible>(key.to_key_value()))
            .expect("field value conversion is infallible")
    }
}

impl<K> From<AccessPath<K>> for AccessPlan<K> {
    fn from(value: AccessPath<K>) -> Self {
        Self::path(value)
    }
}

///
/// SecondaryOrderPushdownEligibility
///
/// Shared eligibility decision for secondary-index ORDER BY pushdown.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SecondaryOrderPushdownEligibility {
    Eligible {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// PushdownApplicability
///
/// Explicit applicability state for secondary-index ORDER BY pushdown.
///
/// This avoids overloading `Option<SecondaryOrderPushdownEligibility>` and
/// keeps "not applicable" separate from "applicable but rejected".
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PushdownApplicability {
    NotApplicable,
    Applicable(SecondaryOrderPushdownEligibility),
}

impl PushdownApplicability {
    /// Return true when this applicability state is eligible for secondary-order pushdown.
    #[must_use]
    pub(crate) const fn is_eligible(&self) -> bool {
        matches!(
            self,
            Self::Applicable(SecondaryOrderPushdownEligibility::Eligible { .. })
        )
    }
}

///
/// PushdownSurfaceEligibility
///
/// Shared conversion boundary from core eligibility into surface-facing
/// projections used by explain and trace layers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PushdownSurfaceEligibility<'a> {
    EligibleSecondaryIndex {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected {
        reason: &'a SecondaryOrderPushdownRejection,
    },
}

impl<'a> From<&'a SecondaryOrderPushdownEligibility> for PushdownSurfaceEligibility<'a> {
    fn from(value: &'a SecondaryOrderPushdownEligibility) -> Self {
        match value {
            SecondaryOrderPushdownEligibility::Eligible { index, prefix_len } => {
                Self::EligibleSecondaryIndex {
                    index,
                    prefix_len: *prefix_len,
                }
            }
            SecondaryOrderPushdownEligibility::Rejected(reason) => Self::Rejected { reason },
        }
    }
}

///
/// SecondaryOrderPushdownRejection
///
/// Deterministic reason why secondary-index ORDER BY pushdown is not eligible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecondaryOrderPushdownRejection {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    AccessPathIndexRangeUnsupported {
        index: &'static str,
        prefix_len: usize,
    },
    InvalidIndexPrefixBounds {
        prefix_len: usize,
        index_field_len: usize,
    },
    MissingPrimaryKeyTieBreak {
        field: String,
    },
    PrimaryKeyDirectionNotAscending {
        field: String,
    },
    MixedDirectionNotEligible {
        field: String,
    },
    OrderFieldsDoNotMatchIndex {
        index: &'static str,
        prefix_len: usize,
        expected_suffix: Vec<String>,
        expected_full: Vec<String>,
        actual: Vec<String>,
    },
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::access::AccessPlan;

    #[test]
    fn union_constructor_flattens_and_collapses_single_child() {
        let plan: AccessPlan<u64> =
            AccessPlan::union(vec![AccessPlan::union(vec![AccessPlan::by_key(7)])]);

        assert_eq!(plan, AccessPlan::by_key(7));
    }

    #[test]
    fn intersection_constructor_flattens_and_collapses_single_child() {
        let plan: AccessPlan<u64> =
            AccessPlan::intersection(vec![AccessPlan::intersection(vec![AccessPlan::by_key(9)])]);

        assert_eq!(plan, AccessPlan::by_key(9));
    }

    #[test]
    fn union_constructor_empty_collapses_to_full_scan() {
        let plan: AccessPlan<u64> = AccessPlan::union(Vec::new());

        assert_eq!(plan, AccessPlan::full_scan());
    }

    #[test]
    fn union_constructor_explicit_empty_is_identity_for_non_empty_children() {
        let plan: AccessPlan<u64> =
            AccessPlan::union(vec![AccessPlan::by_key(7), AccessPlan::by_keys(Vec::new())]);

        assert_eq!(plan, AccessPlan::by_key(7));
    }

    #[test]
    fn union_constructor_only_explicit_empty_children_stays_explicit_empty() {
        let plan: AccessPlan<u64> = AccessPlan::union(vec![
            AccessPlan::by_keys(Vec::new()),
            AccessPlan::by_keys(Vec::new()),
        ]);

        assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
    }

    #[test]
    fn intersection_constructor_empty_collapses_to_full_scan() {
        let plan: AccessPlan<u64> = AccessPlan::intersection(Vec::new());

        assert_eq!(plan, AccessPlan::full_scan());
    }

    #[test]
    fn intersection_constructor_explicit_empty_annihilates_children() {
        let plan: AccessPlan<u64> =
            AccessPlan::intersection(vec![AccessPlan::by_key(9), AccessPlan::by_keys(Vec::new())]);

        assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
    }

    #[test]
    fn intersection_constructor_nested_explicit_empty_annihilates_children() {
        let plan: AccessPlan<u64> = AccessPlan::intersection(vec![AccessPlan::intersection(vec![
            AccessPlan::by_key(9),
            AccessPlan::by_keys(Vec::new()),
        ])]);

        assert_eq!(plan, AccessPlan::by_keys(Vec::new()));
    }
}
