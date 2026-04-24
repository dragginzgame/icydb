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

#[allow(clippy::struct_field_names)]
pub(in crate::db::executor) struct AccessSpecCursor<'a> {
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    index_range_specs: &'a [LoweredIndexRangeSpec],
    index_prefix_offset: usize,
    index_range_offset: usize,
}

impl<'a> AccessSpecCursor<'a> {
    /// Build one spec cursor over explicit lowered prefix/range slices.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        index_range_specs: &'a [LoweredIndexRangeSpec],
    ) -> Self {
        Self {
            index_prefix_specs,
            index_range_specs,
            index_prefix_offset: 0,
            index_range_offset: 0,
        }
    }

    /// Consume the next `count` lowered index-prefix specs in traversal order.
    pub(in crate::db::executor) fn next_index_prefix_specs(
        &mut self,
        count: usize,
    ) -> Option<&'a [LoweredIndexPrefixSpec]> {
        let start = self.index_prefix_offset;
        let end = start.saturating_add(count);
        let slice = self.index_prefix_specs.get(start..end)?;
        self.index_prefix_offset = end;

        Some(slice)
    }

    /// Consume the next `count` lowered index-prefix specs in traversal order,
    /// failing closed when the executable access path requires more specs than
    /// remain available.
    pub(in crate::db::executor) fn require_next_index_prefix_specs(
        &mut self,
        count: usize,
    ) -> Result<&'a [LoweredIndexPrefixSpec], InternalError> {
        self.next_index_prefix_specs(count).ok_or_else(|| {
            InternalError::query_executor_invariant(
                "index-prefix execution requires pre-lowered specs",
            )
        })
    }

    /// Consume the next lowered index-range spec in traversal order.
    pub(in crate::db::executor) fn next_index_range_spec(
        &mut self,
    ) -> Option<&'a LoweredIndexRangeSpec> {
        let spec = self.index_range_specs.get(self.index_range_offset);
        if spec.is_some() {
            self.index_range_offset = self.index_range_offset.saturating_add(1);
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
        if self.index_prefix_offset < self.index_prefix_specs.len() {
            return Err(InternalError::query_executor_invariant(
                "unused index-prefix executable specs after access-plan traversal",
            ));
        }
        validate_index_range_specs_consumed(self.index_range_offset, self.index_range_specs.len())?;

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
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    pub(in crate::db::executor) preserve_leaf_index_order: bool,
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
        Self {
            plan,
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
            preserve_leaf_index_order: false,
        }
    }

    /// Mark this executable access request as one top-level single-path index
    /// scan whose physical traversal order is part of the observable contract.
    #[must_use]
    pub(in crate::db::executor) const fn with_preserved_leaf_index_order(mut self) -> Self {
        self.preserve_leaf_index_order = true;
        self
    }
}

///
/// IndexStreamConstraints
///
/// Canonical constraint envelope for index-backed path resolution.
/// Groups prefix/range controls so call sites pass one structural input rather
/// than multiple loosely related optional arguments.
///

pub(in crate::db) struct IndexStreamConstraints<'a> {
    pub(in crate::db) prefixes: &'a [LoweredIndexPrefixSpec],
    pub(in crate::db) range: Option<&'a LoweredIndexRangeSpec>,
}

///
/// StreamExecutionHints
///
/// Canonical execution-hint envelope for access-path stream production.
/// Keeps bounded fetch and index-predicate pushdown hints grouped and extensible.
///

pub(in crate::db) struct StreamExecutionHints<'a> {
    pub(in crate::db) physical_fetch_hint: Option<usize>,
    pub(in crate::db) predicate_execution: Option<IndexPredicateExecution<'a>>,
}
