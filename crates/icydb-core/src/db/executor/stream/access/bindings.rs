//! Module: executor::stream::access::bindings
//! Responsibility: canonical access-stream binding contracts for executor traversal setup.
//! Does not own: access-path planning semantics or runtime row materialization policy.
//! Boundary: provides typed stream input/output contracts across execution seams.

use crate::{
    db::{
        access::AccessPlan,
        cursor::IndexScanContinuationInput,
        direction::Direction,
        executor::{
            ExecutableAccessPlan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey,
            traversal::IndexRangeTraversalContract,
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

#[expect(clippy::struct_field_names)]
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
/// AccessStreamBindings
///
/// Shared access-stream traversal bindings reused by execution and key-stream
/// request wrappers so spec + continuation fields stay aligned.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessStreamBindings<'a> {
    pub(in crate::db::executor) index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'a [LoweredIndexRangeSpec],
    pub(in crate::db::executor) continuation: AccessScanContinuationInput<'a>,
}

impl<'a> AccessStreamBindings<'a> {
    /// Build one access-stream binding envelope with explicit lowered-spec slices.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        index_range_specs: &'a [LoweredIndexRangeSpec],
        continuation: AccessScanContinuationInput<'a>,
    ) -> Self {
        Self {
            index_prefix_specs,
            index_range_specs,
            continuation,
        }
    }

    /// Build one binding envelope with no index-lowered specs.
    #[must_use]
    pub(in crate::db::executor) const fn no_index(direction: Direction) -> Self {
        Self::new(&[], &[], AccessScanContinuationInput::new(None, direction))
    }

    /// Build one binding envelope for one index-prefix spec.
    #[must_use]
    pub(in crate::db::executor) const fn with_index_prefix(
        index_prefix_spec: &'a LoweredIndexPrefixSpec,
        direction: Direction,
    ) -> Self {
        Self::new(
            std::slice::from_ref(index_prefix_spec),
            &[],
            AccessScanContinuationInput::new(None, direction),
        )
    }

    /// Build one binding envelope for one index-range spec with explicit continuation contract.
    #[must_use]
    pub(in crate::db::executor) const fn with_index_range_continuation(
        index_range_spec: &'a LoweredIndexRangeSpec,
        continuation: AccessScanContinuationInput<'a>,
    ) -> Self {
        Self::new(&[], std::slice::from_ref(index_range_spec), continuation)
    }

    /// Borrow continuation scan direction from this binding envelope.
    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.continuation.direction()
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
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, K> ExecutableAccess<'a, K> {
    /// Build one canonical runtime request from one structural access plan.
    #[must_use]
    pub(in crate::db::executor) fn new(
        access: &'a AccessPlan<K>,
        bindings: AccessStreamBindings<'a>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self::from_executable_plan(
            access.resolve_strategy().into_executable(),
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        )
    }

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

pub(in crate::db) struct IndexStreamConstraints<'a> {
    pub(in crate::db) prefixes: &'a [LoweredIndexPrefixSpec],
    pub(in crate::db) range: Option<&'a LoweredIndexRangeSpec>,
}

///
/// AccessScanContinuationInput
///
/// Access-stream continuation traversal bindings.
/// Stores one index-layer continuation contract so stream/access boundaries
/// forward continuation semantics without interpreting anchor primitives.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct AccessScanContinuationInput<'a> {
    index_scan_continuation: IndexScanContinuationInput<'a>,
}

impl<'a> AccessScanContinuationInput<'a> {
    /// Build one access-scan continuation input.
    #[must_use]
    pub(in crate::db) const fn new(anchor: Option<&'a LoweredKey>, direction: Direction) -> Self {
        Self {
            index_scan_continuation: IndexScanContinuationInput::new(anchor, direction),
        }
    }

    /// Build one initial (non-continuation) ascending scan continuation input.
    #[must_use]
    pub(in crate::db) const fn initial_asc() -> Self {
        Self::new(None, Direction::Asc)
    }

    /// Borrow continuation scan direction.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.index_scan_continuation.direction()
    }

    /// Build one index-scan continuation contract for index-layer traversal.
    #[must_use]
    pub(in crate::db) const fn index_scan_continuation(&self) -> IndexScanContinuationInput<'a> {
        self.index_scan_continuation
    }
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
