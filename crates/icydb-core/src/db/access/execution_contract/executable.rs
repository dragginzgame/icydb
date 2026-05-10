//! Module: access::execution_contract::executable
//! Responsibility: normalized executable access-path contract representation for traversal.
//! Does not own: planner access-path derivation or executor route precedence policy.
//! Boundary: carries compact executable access metadata consumed by runtime traversal layers.

use crate::db::access::{AccessPath, AccessPlan, execution_contract::ExecutionPathPayload};

///
/// ExecutableAccessNode
///
/// Recursive normalized execution tree for one access plan.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutableAccessNode<'a, K> {
    Path(ExecutionPathPayload<'a, K>),
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
    /// Project one semantic access plan into its normalized execution contract.
    #[must_use]
    pub(in crate::db) fn from_access_plan(access: &'a AccessPlan<K>) -> Self {
        match access {
            AccessPlan::Path(path) => Self::from_access_path(path.as_ref()),
            AccessPlan::Union(children) => {
                Self::union(Self::from_access_plan_children(children.as_slice()))
            }
            AccessPlan::Intersection(children) => {
                Self::intersection(Self::from_access_plan_children(children.as_slice()))
            }
        }
    }

    /// Project one semantic access path into a path-backed execution contract.
    #[must_use]
    pub(in crate::db::access) fn from_access_path(path: &'a AccessPath<K>) -> Self {
        Self::for_path(ExecutionPathPayload::from_access_path(path))
    }

    /// Construct one path-backed executable access plan.
    #[must_use]
    pub(in crate::db) const fn for_path(path: ExecutionPathPayload<'a, K>) -> Self {
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
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutionPathPayload<'a, K>> {
        match &self.node {
            ExecutableAccessNode::Path(path) => Some(path),
            ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
        }
    }

    // Project a child list while preserving canonical child ordering.
    fn from_access_plan_children(children: &'a [AccessPlan<K>]) -> Vec<Self> {
        children.iter().map(Self::from_access_plan).collect()
    }
}
