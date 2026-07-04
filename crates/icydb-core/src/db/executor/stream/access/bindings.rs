//! Module: executor::stream::access::bindings
//! Responsibility: access-stream traversal cursors and executable stream wrappers.
//! Does not own: access-path planning semantics or runtime row materialization policy.
//! Boundary: keeps stream behavior next to physical access traversal.

use crate::{
    db::{
        executor::{
            ExecutableAccessPlan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            pipeline::contracts::AccessStreamBindings, traversal::IndexRangeTraversalContract,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
};

///
/// AccessSpecCursor
///
/// Mutable traversal cursor for index prefix/range specs while walking an access plan.
/// Keeps consumption order explicit and exposes one end-of-traversal invariant check.
///

pub(in crate::db::executor) struct AccessSpecCursor<'a> {
    prefixes: &'a [LoweredIndexPrefixSpec],
    ranges: &'a [LoweredIndexRangeSpec],
    prefix_offset: usize,
    range_offset: usize,
}

impl<'a> AccessSpecCursor<'a> {
    /// Build one spec cursor over explicit lowered prefix/range slices.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        index_range_specs: &'a [LoweredIndexRangeSpec],
    ) -> Self {
        Self {
            prefixes: index_prefix_specs,
            ranges: index_range_specs,
            prefix_offset: 0,
            range_offset: 0,
        }
    }

    /// Consume the next `count` lowered index-prefix specs in traversal order.
    pub(in crate::db::executor) fn next_index_prefix_specs(
        &mut self,
        count: usize,
    ) -> Option<&'a [LoweredIndexPrefixSpec]> {
        let start = self.prefix_offset;
        let end = start.saturating_add(count);
        let slice = self.prefixes.get(start..end)?;
        self.prefix_offset = end;

        Some(slice)
    }

    /// Consume the next `count` lowered index-prefix specs in traversal order,
    /// failing closed when the executable access path requires more specs than
    /// remain available.
    pub(in crate::db::executor) fn require_next_index_prefix_specs(
        &mut self,
        count: usize,
    ) -> Result<&'a [LoweredIndexPrefixSpec], InternalError> {
        self.next_index_prefix_specs(count)
            .ok_or_else(InternalError::query_executor_invariant)
    }

    /// Consume the next lowered index-range spec in traversal order.
    pub(in crate::db::executor) fn next_index_range_spec(
        &mut self,
    ) -> Option<&'a LoweredIndexRangeSpec> {
        let spec = self.ranges.get(self.range_offset);
        if spec.is_some() {
            self.range_offset = self.range_offset.saturating_add(1);
        }

        spec
    }

    /// Consume the next lowered index-range spec in traversal order, failing
    /// closed when traversal requires an index-range spec that was not lowered.
    pub(in crate::db::executor) fn require_next_index_range_spec(
        &mut self,
    ) -> Result<&'a LoweredIndexRangeSpec, InternalError> {
        IndexRangeTraversalContract::require_spec(self.next_index_range_spec())
    }

    /// Enforce that all lowered specs were consumed during access-plan traversal.
    pub(in crate::db::executor) fn validate_consumed(&self) -> Result<(), InternalError> {
        if self.prefix_offset < self.prefixes.len() {
            return Err(InternalError::query_executor_invariant());
        }
        validate_index_range_specs_consumed(self.range_offset, self.ranges.len())?;

        Ok(())
    }
}

// Keep the historical bindings-layer invariant name stable for CI checks while
// routing the actual contract enforcement through the traversal owner.
fn validate_index_range_specs_consumed(
    consumed: usize,
    available: usize,
) -> Result<(), InternalError> {
    IndexRangeTraversalContract::validate_specs_consumed(consumed, available)
}

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
    /// Build one canonical runtime request from one executable access plan.
    #[must_use]
    pub(in crate::db::executor) const fn from_executable_plan(
        plan: ExecutableAccessPlan<'a, K>,
        bindings: AccessStreamBindings<'a>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self::from_executable_plan_with_policy(
            plan,
            bindings,
            AccessStreamExecutionPolicy::canonical_key_order(physical_fetch_hint),
            index_predicate_execution,
        )
    }

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

///
/// IndexStreamConstraints
///
/// Canonical constraint envelope for index-backed path resolution.
/// Groups prefix/range controls so call sites pass one structural input rather
/// than multiple loosely related optional arguments.
///

pub(in crate::db::executor) struct IndexStreamConstraints<'a> {
    pub(in crate::db::executor) prefixes: &'a [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) range: Option<&'a LoweredIndexRangeSpec>,
}
