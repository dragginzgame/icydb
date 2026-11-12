//! Module: access::execution_contract::executable
//! Responsibility: borrowed executable access contracts for traversal.
//! Does not own: planner access selection or executor route precedence.
//! Boundary: project mechanical leaf payloads without rebuilding the access tree.

use crate::db::access::{
    AccessPlan, execution_contract::ExecutionPathPayload, lowering::LoweredIndexSpecs,
};

/// Mechanical access contract borrowing the canonical semantic tree.
/// Composite children are projected on demand, never copied into another tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutableAccessPlan<'a, K> {
    Path(ExecutionPathPayload<'a, K>, Option<&'a LoweredIndexSpecs>),
    Union(ExecutableAccessChildren<'a, K>),
    Intersection(ExecutableAccessChildren<'a, K>),
}

/// Borrowed children expose only executable projections to traversal consumers.
/// Keeping the semantic slice private preserves the planner/runtime boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutableAccessChildren<'a, K>(&'a [AccessPlan<K>]);

impl<'a, K> ExecutableAccessChildren<'a, K> {
    /// Return the number of children without projecting their payloads.
    #[must_use]
    pub(in crate::db) const fn len(&self) -> usize {
        self.0.len()
    }

    /// Project direct children in canonical order without allocating backing.
    pub(in crate::db) fn iter(&self) -> impl ExactSizeIterator<Item = ExecutableAccessPlan<'a, K>> {
        self.0.iter().map(ExecutableAccessPlan::from_access_plan)
    }
}

impl<'a, K> ExecutableAccessPlan<'a, K> {
    /// Borrow one semantic access plan as an executable contract.
    #[must_use]
    pub(in crate::db::access) fn from_access_plan(access: &'a AccessPlan<K>) -> Self {
        match access {
            AccessPlan::Path(path, specs) => Self::Path(
                ExecutionPathPayload::from_access_path(path),
                specs.as_deref(),
            ),
            AccessPlan::Union(children) => Self::Union(ExecutableAccessChildren(children)),
            AccessPlan::Intersection(children) => {
                Self::Intersection(ExecutableAccessChildren(children))
            }
        }
    }

    /// Borrow physical bounds from this leaf, rejecting unprepared index access.
    pub(in crate::db) fn index_specs(
        &self,
    ) -> Result<Option<&'a LoweredIndexSpecs>, crate::error::InternalError> {
        match self {
            Self::Path(_, Some(specs)) => Ok(Some(specs)),
            Self::Path(
                ExecutionPathPayload::ByKey(_)
                | ExecutionPathPayload::ByKeys(_)
                | ExecutionPathPayload::KeyRange { .. }
                | ExecutionPathPayload::FullScan,
                None,
            ) => Ok(None),
            _ => Err(crate::error::InternalError::query_executor_invariant()),
        }
    }

    /// Borrow the leaf payload when this contract represents one path.
    #[must_use]
    pub(in crate::db) const fn as_path(&self) -> Option<&ExecutionPathPayload<'a, K>> {
        match self {
            Self::Path(path, _) => Some(path),
            Self::Union(_) | Self::Intersection(_) => None,
        }
    }
}
