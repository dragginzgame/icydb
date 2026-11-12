//! Module: executor::stream::access::bindings
//! Responsibility: access-stream execution policy and executable stream wrappers.
//! Does not own: access-path planning semantics or runtime row materialization policy.
//! Boundary: keeps stream behavior next to physical access traversal.

use crate::db::{
    executor::{ExecutableAccessPlan, pipeline::contracts::AccessStreamBindings},
    index::predicate::IndexPredicateExecution,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::executor) enum IndexLeafOrderPolicy {
    CanonicalKey,
    PreservePhysicalLeaf,
    PreservePrefixBranch,
}

impl IndexLeafOrderPolicy {
    #[must_use]
    pub(in crate::db::executor) const fn preserves_leaf_index_order(self) -> bool {
        matches!(
            self,
            Self::PreservePhysicalLeaf | Self::PreservePrefixBranch
        )
    }

    #[must_use]
    pub(in crate::db::executor) const fn preserves_prefix_branch_order(self) -> bool {
        matches!(self, Self::PreservePrefixBranch)
    }
}

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessStreamExecutionPolicy {
    physical_fetch_hint: Option<usize>,
    index_leaf_order_policy: IndexLeafOrderPolicy,
}

impl AccessStreamExecutionPolicy {
    #[must_use]
    pub(in crate::db::executor) const fn new(
        physical_fetch_hint: Option<usize>,
        index_leaf_order_policy: IndexLeafOrderPolicy,
    ) -> Self {
        Self {
            physical_fetch_hint,
            index_leaf_order_policy,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn canonical_key_order(
        physical_fetch_hint: Option<usize>,
    ) -> Self {
        Self::new(physical_fetch_hint, IndexLeafOrderPolicy::CanonicalKey)
    }

    #[must_use]
    pub(in crate::db::executor) const fn physical_fetch_hint(self) -> Option<usize> {
        self.physical_fetch_hint
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_leaf_order_policy(self) -> IndexLeafOrderPolicy {
        self.index_leaf_order_policy
    }

    #[must_use]
    pub(in crate::db::executor) const fn with_physical_fetch_hint(
        self,
        physical_fetch_hint: Option<usize>,
    ) -> Self {
        Self {
            physical_fetch_hint,
            ..self
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn with_index_leaf_order_policy(
        self,
        index_leaf_order_policy: IndexLeafOrderPolicy,
    ) -> Self {
        Self {
            index_leaf_order_policy,
            ..self
        }
    }
}

///
/// ExecutableAccess
///
/// Canonical runtime executable-access request for key-stream production.
/// This owns one executable access plan plus stream bindings/hints, so route
/// and executor layers pass one concrete runtime contract.
///

pub(in crate::db::executor) struct ExecutableAccess<'a, K> {
    pub(in crate::db::executor) plan: ExecutableAccessPlan<'a, K>,
    pub(in crate::db::executor) bindings: AccessStreamBindings<'a>,
    pub(in crate::db::executor) execution_policy: AccessStreamExecutionPolicy,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, K> ExecutableAccess<'a, K> {
    /// Build one runtime request from one executable access plan and explicit
    /// stream execution policy.
    #[must_use]
    pub(in crate::db::executor) const fn from_executable_plan_with_policy(
        plan: ExecutableAccessPlan<'a, K>,
        bindings: AccessStreamBindings<'a>,
        execution_policy: AccessStreamExecutionPolicy,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self {
            plan,
            bindings,
            execution_policy,
            index_predicate_execution,
        }
    }
}
