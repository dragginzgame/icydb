//! Module: access::execution_contract::executable
//! Responsibility: normalized executable access-path contract representation for traversal.
//! Does not own: planner access-path derivation or executor route precedence policy.
//! Boundary: carries compact executable access metadata consumed by runtime traversal layers.

use crate::{
    db::access::{
        AccessPathKind, ExecutableAccessPathDispatch, dispatch_executable_access_path,
        execution_contract::{
            AccessExecutionMode, ExecutionBounds, ExecutionDistinctMode, ExecutionOrdering,
            ExecutionPathPayload,
        },
    },
    model::index::IndexModel,
    value::Value,
};
use std::ops::Bound;

///
/// ExecutableAccessPath
///
/// Normalized execution contract for one concrete access path.
/// Holds compact execution mechanics plus variant payload needed for traversal.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessPath<'a, K> {
    mode: AccessExecutionMode,
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
        mode: AccessExecutionMode,
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
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
        match dispatch_executable_access_path(self) {
            ExecutableAccessPathDispatch::ByKey(_) => AccessPathKind::ByKey,
            ExecutableAccessPathDispatch::ByKeys(_) => AccessPathKind::ByKeys,
            ExecutableAccessPathDispatch::KeyRange { .. } => AccessPathKind::KeyRange,
            ExecutableAccessPathDispatch::IndexPrefix => AccessPathKind::IndexPrefix,
            ExecutableAccessPathDispatch::IndexMultiLookup { .. } => {
                AccessPathKind::IndexMultiLookup
            }
            ExecutableAccessPathDispatch::IndexRange => AccessPathKind::IndexRange,
            ExecutableAccessPathDispatch::FullScan => AccessPathKind::FullScan,
        }
    }

    /// Return the coarse execution mode.
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> AccessExecutionMode {
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
            | ExecutionPathPayload::IndexMultiLookup { .. }
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
    pub(in crate::db) mode: AccessExecutionMode,
    pub(in crate::db) ordering: ExecutionOrdering,
    pub(in crate::db) bounds: ExecutionBounds,
    pub(in crate::db) distinct: ExecutionDistinctMode,
    pub(in crate::db) requires_decoded_id: bool,
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
            mode: AccessExecutionMode::Composite,
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
            mode: AccessExecutionMode::Intersect,
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
