//! Module: db::access::execution_contract
//! Responsibility: shared normalized access contracts consumed by query/cursor/executor.
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
