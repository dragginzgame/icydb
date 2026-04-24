//! Module: access::execution_contract::executable
//! Responsibility: normalized executable access-path contract representation for traversal.
//! Does not own: planner access-path derivation or executor route precedence policy.
//! Boundary: carries compact executable access metadata consumed by runtime traversal layers.

use crate::{
    db::access::{
        AccessPathKind,
        execution_contract::{ExecutionBounds, ExecutionPathPayload},
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
    bounds: ExecutionBounds,
    payload: ExecutionPathPayload<'a, K>,
}

impl<'a, K> ExecutableAccessPath<'a, K> {
    /// Construct a normalized executable-path contract.
    #[must_use]
    pub(in crate::db) const fn new(
        bounds: ExecutionBounds,
        payload: ExecutionPathPayload<'a, K>,
    ) -> Self {
        Self { bounds, payload }
    }

    /// Borrow the execution payload for this path.
    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &ExecutionPathPayload<'a, K> {
        &self.payload
    }

    /// Return the canonical execution path kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
        match self.payload {
            ExecutionPathPayload::ByKey(_) => AccessPathKind::ByKey,
            ExecutionPathPayload::ByKeys(_) => AccessPathKind::ByKeys,
            ExecutionPathPayload::KeyRange { .. } => AccessPathKind::KeyRange,
            ExecutionPathPayload::IndexPrefix => AccessPathKind::IndexPrefix,
            ExecutionPathPayload::IndexMultiLookup { .. } => AccessPathKind::IndexMultiLookup,
            ExecutionPathPayload::IndexRange { .. } => AccessPathKind::IndexRange,
            ExecutionPathPayload::FullScan => AccessPathKind::FullScan,
        }
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
    node: ExecutableAccessNode<'a, K>,
}

impl<'a, K> ExecutableAccessPlan<'a, K> {
    /// Construct one path-backed executable access plan.
    #[must_use]
    pub(in crate::db) const fn for_path(path: ExecutableAccessPath<'a, K>) -> Self {
        Self {
            node: ExecutableAccessNode::Path(path),
        }
    }

    /// Construct one union executable access plan.
    #[must_use]
    pub(in crate::db) const fn union(children: Vec<Self>) -> Self {
        Self {
            node: ExecutableAccessNode::Union(children),
        }
    }

    /// Construct one intersection executable access plan.
    #[must_use]
    pub(in crate::db) const fn intersection(children: Vec<Self>) -> Self {
        Self {
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
