//! Module: db::executor::access_contract
//! Responsibility: executor-owned normalized access contracts consumed at runtime.
//! Does not own: logical access-path selection policy.
//! Boundary: planner lowers `AccessPlan`/`AccessPath` into these execution mechanics.

use crate::{db::direction::Direction, model::index::IndexModel, value::Value};
use std::ops::Bound;

// Audit Summary:
// - `path: &AccessPath<K>` was previously used only by stream physical lowering.
// - `index_prefix_details`, `index_range_details`, and `index_fields_for_slot_map` duplicated
//   data already available in `ExecutionBounds`.
// - Behavioral `AccessPath` matching in executor runtime has been removed in favor of
//   `ExecutableAccessPath` payload + mechanical execution fields.

///
/// ExecutionMode
///
/// Coarse execution mode used by executor routing.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionMode {
    FullScan,
    IndexRange,
    OrderedIndexScan,
    Intersect,
    Composite,
}

///
/// ExecutionOrdering
///
/// Ordering contract required by executor traversal mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionOrdering {
    Natural,
    ByIndex(Direction),
}

///
/// ExecutionDistinctMode
///
/// Distinct handling mode required by execution mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionDistinctMode {
    None,
    PreOrdered,
    RequiresMaterialization,
}

///
/// ExecutionBounds
///
/// Minimal bound shape required by executor path mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionBounds {
    Unbounded,
    PrimaryKeyRange,
    IndexPrefix {
        index: IndexModel,
        prefix_len: usize,
    },
    IndexRange {
        index: IndexModel,
        prefix_len: usize,
    },
}

///
/// ExecutionPathKind
///
/// Canonical path discriminant used by executor runtime checks.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionPathKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexRange,
    FullScan,
}

///
/// ExecutionPathPayload
///
/// Variant payload needed for mechanical access execution only.
/// This contract intentionally excludes planner semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionPathPayload<'a, K> {
    ByKey(&'a K),
    ByKeys(&'a [K]),
    KeyRange {
        start: &'a K,
        end: &'a K,
    },
    IndexPrefix,
    IndexRange {
        prefix_values: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    FullScan,
}

///
/// ExecutableAccessPath
///
/// Normalized execution contract for one concrete access path.
/// Holds compact execution mechanics plus variant payload needed for traversal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessPath<'a, K> {
    mode: ExecutionMode,
    ordering: ExecutionOrdering,
    bounds: ExecutionBounds,
    distinct: ExecutionDistinctMode,
    requires_decoded_id: bool,
    payload: ExecutionPathPayload<'a, K>,
}

impl<'a, K> ExecutableAccessPath<'a, K> {
    /// Construct a normalized executable-path contract.
    #[must_use]
    pub(in crate::db) const fn new(
        mode: ExecutionMode,
        ordering: ExecutionOrdering,
        bounds: ExecutionBounds,
        distinct: ExecutionDistinctMode,
        requires_decoded_id: bool,
        payload: ExecutionPathPayload<'a, K>,
    ) -> Self {
        Self {
            mode,
            ordering,
            bounds,
            distinct,
            requires_decoded_id,
            payload,
        }
    }

    /// Borrow the execution payload for this path.
    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &ExecutionPathPayload<'a, K> {
        &self.payload
    }

    /// Return the canonical execution path kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> ExecutionPathKind {
        match self.payload {
            ExecutionPathPayload::ByKey(_) => ExecutionPathKind::ByKey,
            ExecutionPathPayload::ByKeys(_) => ExecutionPathKind::ByKeys,
            ExecutionPathPayload::KeyRange { .. } => ExecutionPathKind::KeyRange,
            ExecutionPathPayload::IndexPrefix => ExecutionPathKind::IndexPrefix,
            ExecutionPathPayload::IndexRange { .. } => ExecutionPathKind::IndexRange,
            ExecutionPathPayload::FullScan => ExecutionPathKind::FullScan,
        }
    }

    /// Return the coarse execution mode.
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Return ordering mechanics for this path.
    #[must_use]
    pub(in crate::db) const fn ordering(&self) -> ExecutionOrdering {
        self.ordering
    }

    /// Return bound mechanics for this path.
    #[must_use]
    pub(in crate::db) const fn bounds(&self) -> ExecutionBounds {
        self.bounds
    }

    /// Return distinct mode for this path.
    #[must_use]
    pub(in crate::db) const fn distinct(&self) -> ExecutionDistinctMode {
        self.distinct
    }

    /// Return whether this path requires decoded-id materialization.
    #[must_use]
    pub(in crate::db) const fn requires_decoded_id(&self) -> bool {
        self.requires_decoded_id
    }

    /// Return true when this path can use primary-key stream fast-path access.
    /// This indicates access-shape feasibility, not output-order guarantees.
    #[must_use]
    pub(in crate::db) const fn supports_pk_stream_access(&self) -> bool {
        matches!(
            self.kind(),
            ExecutionPathKind::KeyRange | ExecutionPathKind::FullScan
        )
    }

    /// Return true when this path supports count pushdown shape.
    #[must_use]
    pub(in crate::db) const fn supports_count_pushdown_shape(&self) -> bool {
        matches!(
            self.kind(),
            ExecutionPathKind::KeyRange | ExecutionPathKind::FullScan
        )
    }

    /// Return true when this path supports primary-scan fetch hints.
    #[must_use]
    pub(in crate::db) const fn supports_primary_scan_fetch_hint(&self) -> bool {
        matches!(
            self.kind(),
            ExecutionPathKind::ByKey | ExecutionPathKind::KeyRange | ExecutionPathKind::FullScan
        )
    }

    /// Return true when this path supports reverse traversal.
    #[must_use]
    pub(in crate::db) const fn supports_reverse_traversal(&self) -> bool {
        matches!(
            self.kind(),
            ExecutionPathKind::ByKey
                | ExecutionPathKind::KeyRange
                | ExecutionPathKind::IndexPrefix
                | ExecutionPathKind::IndexRange
                | ExecutionPathKind::FullScan
        )
    }

    /// Return true when this path preserves PK stream ordering.
    /// This indicates output-order guarantees, not fast-path access feasibility.
    #[must_use]
    #[expect(clippy::unused_self)]
    pub(in crate::db) const fn is_pk_ordered_stream(&self) -> bool {
        true
    }

    /// Return true when this path is direct key access (`ByKey` / `ByKeys`).
    #[must_use]
    pub(in crate::db) const fn is_key_direct_access(&self) -> bool {
        matches!(
            self.payload,
            ExecutionPathPayload::ByKey(_) | ExecutionPathPayload::ByKeys(_)
        )
    }

    /// Return true when this path is an empty `ByKeys`.
    #[must_use]
    pub(in crate::db) const fn is_by_keys_empty(&self) -> bool {
        matches!(self.payload, ExecutionPathPayload::ByKeys(keys) if keys.is_empty())
    }

    /// Borrow semantic index-range bounds required for cursor envelope validation.
    #[must_use]
    pub(in crate::db) const fn index_range_semantic_bounds(
        &self,
    ) -> Option<(&'a [Value], &'a Bound<Value>, &'a Bound<Value>)> {
        match self.payload {
            ExecutionPathPayload::IndexRange {
                prefix_values,
                lower,
                upper,
            } => Some((prefix_values, lower, upper)),
            ExecutionPathPayload::ByKey(_)
            | ExecutionPathPayload::ByKeys(_)
            | ExecutionPathPayload::KeyRange { .. }
            | ExecutionPathPayload::IndexPrefix
            | ExecutionPathPayload::FullScan => None,
        }
    }

    /// Borrow index-prefix details when this path is index-prefix.
    #[must_use]
    pub(in crate::db) const fn index_prefix_details(&self) -> Option<(IndexModel, usize)> {
        match self.bounds {
            ExecutionBounds::IndexPrefix { index, prefix_len } => Some((index, prefix_len)),
            ExecutionBounds::Unbounded
            | ExecutionBounds::PrimaryKeyRange
            | ExecutionBounds::IndexRange { .. } => None,
        }
    }

    /// Borrow index-range details when this path is index-range.
    #[must_use]
    pub(in crate::db) const fn index_range_details(&self) -> Option<(IndexModel, usize)> {
        match self.bounds {
            ExecutionBounds::IndexRange { index, prefix_len } => Some((index, prefix_len)),
            ExecutionBounds::Unbounded
            | ExecutionBounds::PrimaryKeyRange
            | ExecutionBounds::IndexPrefix { .. } => None,
        }
    }

    /// Borrow index fields used to build predicate slot maps when index-backed.
    #[must_use]
    pub(in crate::db) const fn index_fields_for_slot_map(&self) -> Option<&'static [&'static str]> {
        match self.bounds {
            ExecutionBounds::IndexPrefix { index, .. }
            | ExecutionBounds::IndexRange { index, .. } => Some(index.fields),
            ExecutionBounds::Unbounded | ExecutionBounds::PrimaryKeyRange => None,
        }
    }

    /// Return true when this path consumes one lowered index-prefix spec.
    #[must_use]
    pub(in crate::db) const fn consumes_index_prefix_spec(&self) -> bool {
        matches!(self.bounds, ExecutionBounds::IndexPrefix { .. })
    }

    /// Return true when this path consumes one lowered index-range spec.
    #[must_use]
    pub(in crate::db) const fn consumes_index_range_spec(&self) -> bool {
        matches!(self.bounds, ExecutionBounds::IndexRange { .. })
    }
}

///
/// ExecutableAccessNode
///
/// Recursive normalized execution tree for one access plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutableAccessNode<'a, K> {
    Path(ExecutableAccessPath<'a, K>),
    Union(Vec<ExecutableAccessPlan<'a, K>>),
    Intersection(Vec<ExecutableAccessPlan<'a, K>>),
}

///
/// ExecutableAccessPlan
///
/// Normalized execution contract for one access plan.
/// This is executor-consumed and planner-lowered.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessPlan<'a, K> {
    pub mode: ExecutionMode,
    pub ordering: ExecutionOrdering,
    pub bounds: ExecutionBounds,
    pub distinct: ExecutionDistinctMode,
    pub requires_decoded_id: bool,
    node: ExecutableAccessNode<'a, K>,
}

impl<'a, K> ExecutableAccessPlan<'a, K> {
    /// Construct one path-backed executable access plan.
    #[must_use]
    pub(in crate::db) const fn for_path(path: ExecutableAccessPath<'a, K>) -> Self {
        Self {
            mode: path.mode(),
            ordering: path.ordering(),
            bounds: path.bounds(),
            distinct: path.distinct(),
            requires_decoded_id: path.requires_decoded_id(),
            node: ExecutableAccessNode::Path(path),
        }
    }

    /// Construct one union executable access plan.
    #[must_use]
    pub(in crate::db) fn union(children: Vec<Self>) -> Self {
        Self {
            mode: ExecutionMode::Composite,
            ordering: ExecutionOrdering::Natural,
            bounds: ExecutionBounds::Unbounded,
            distinct: ExecutionDistinctMode::RequiresMaterialization,
            requires_decoded_id: children.iter().any(|child| child.requires_decoded_id),
            node: ExecutableAccessNode::Union(children),
        }
    }

    /// Construct one intersection executable access plan.
    #[must_use]
    pub(in crate::db) fn intersection(children: Vec<Self>) -> Self {
        Self {
            mode: ExecutionMode::Intersect,
            ordering: ExecutionOrdering::Natural,
            bounds: ExecutionBounds::Unbounded,
            distinct: ExecutionDistinctMode::RequiresMaterialization,
            requires_decoded_id: children.iter().any(|child| child.requires_decoded_id),
            node: ExecutableAccessNode::Intersection(children),
        }
    }

    /// Borrow the normalized execution tree node.
    #[must_use]
    pub(in crate::db) const fn node(&self) -> &ExecutableAccessNode<'a, K> {
        &self.node
    }

    /// Borrow path execution contract when this plan is one path node.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutableAccessPath<'a, K>> {
        match &self.node {
            ExecutableAccessNode::Path(path) => Some(path),
            ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
        }
    }
}
