use crate::{
    db::access::{AccessPath, IndexRangePathRef},
    model::index::IndexModel,
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
        for child in children {
            Self::append_union_flattened(&mut out, child);
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
            Self::append_intersection_flattened(&mut out, child);
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

    /// Borrow index-prefix access details when this is a single IndexPrefix path.
    #[must_use]
    pub(crate) fn as_index_prefix_path(&self) -> Option<(&IndexModel, &[Value])> {
        self.as_path().and_then(AccessPath::as_index_prefix)
    }

    /// Borrow index-range access details when this is a single IndexRange path.
    #[must_use]
    pub(crate) fn as_index_range_path(&self) -> Option<IndexRangePathRef<'_>> {
        self.as_path().and_then(AccessPath::as_index_range)
    }

    /// Walk the tree and return the first encountered IndexRange details.
    #[must_use]
    pub(crate) fn first_index_range_details(&self) -> Option<(&IndexModel, usize)> {
        match self {
            Self::Path(path) => path.index_range_details(),
            Self::Union(children) | Self::Intersection(children) => {
                children.iter().find_map(Self::first_index_range_details)
            }
        }
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

    // Append one child into a flattened union accumulator.
    fn append_union_flattened(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Union(children) => {
                for child in children {
                    Self::append_union_flattened(out, child);
                }
            }
            other => out.push(other),
        }
    }

    // Append one child into a flattened intersection accumulator.
    fn append_intersection_flattened(out: &mut Vec<Self>, child: Self) {
        match child {
            Self::Intersection(children) => {
                for child in children {
                    Self::append_intersection_flattened(out, child);
                }
            }
            other => out.push(other),
        }
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
pub enum PushdownSurfaceEligibility<'a> {
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
    use crate::db::access::{AccessPath, AccessPlan};

    #[test]
    fn union_constructor_flattens_and_collapses_single_child() {
        let plan: AccessPlan<u64> =
            AccessPlan::union(vec![AccessPlan::union(vec![AccessPlan::path(
                AccessPath::ByKey(7),
            )])]);

        assert_eq!(plan, AccessPlan::path(AccessPath::ByKey(7)));
    }

    #[test]
    fn intersection_constructor_flattens_and_collapses_single_child() {
        let plan: AccessPlan<u64> =
            AccessPlan::intersection(vec![AccessPlan::intersection(vec![AccessPlan::path(
                AccessPath::ByKey(9),
            )])]);

        assert_eq!(plan, AccessPlan::path(AccessPath::ByKey(9)));
    }

    #[test]
    fn union_constructor_empty_collapses_to_full_scan() {
        let plan: AccessPlan<u64> = AccessPlan::union(Vec::new());

        assert_eq!(plan, AccessPlan::full_scan());
    }

    #[test]
    fn intersection_constructor_empty_collapses_to_full_scan() {
        let plan: AccessPlan<u64> = AccessPlan::intersection(Vec::new());

        assert_eq!(plan, AccessPlan::full_scan());
    }
}
