use crate::{
    db::{
        access::{AccessPlan, lower_executable_access_plan},
        direction::Direction,
        executor::{
            Context, ExecutableAccessPlan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            LoweredKey,
        },
        index::{IndexScanContinuationInput, predicate::IndexPredicateExecution},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// AccessStreamInputs
///
/// Canonical access-stream construction inputs shared across context/composite boundaries.
/// This bundles spec slices and traversal controls to avoid argument-order drift.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessStreamInputs<'ctx, 'a, E: EntityKind + EntityValue> {
    pub(in crate::db::executor) ctx: &'a Context<'ctx, E>,
    pub(in crate::db::executor) index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'a [LoweredIndexRangeSpec],
    pub(in crate::db::executor) continuation: AccessScanContinuationInput<'a>,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, E> AccessStreamInputs<'_, 'a, E>
where
    E: EntityKind + EntityValue,
{
    /// Clone this envelope with one overridden physical fetch hint.
    #[must_use]
    pub(in crate::db::executor) const fn with_physical_fetch_hint(
        &self,
        physical_fetch_hint: Option<usize>,
    ) -> Self {
        Self {
            ctx: self.ctx,
            index_prefix_specs: self.index_prefix_specs,
            index_range_specs: self.index_range_specs,
            continuation: self.continuation,
            physical_fetch_hint,
            index_predicate_execution: self.index_predicate_execution,
        }
    }

    // Build one mutable spec-consumption cursor over prefix/range slices.
    #[must_use]
    pub(super) fn spec_cursor(&self) -> AccessSpecCursor<'a> {
        AccessSpecCursor {
            index_prefix_specs: self.index_prefix_specs.iter(),
            index_range_specs: self.index_range_specs.iter(),
        }
    }
}

///
/// AccessSpecCursor
///
/// Mutable traversal cursor for index prefix/range specs while walking an access plan.
/// Keeps consumption order explicit and exposes one end-of-traversal invariant check.
///

pub(in crate::db::executor) struct AccessSpecCursor<'a> {
    index_prefix_specs: std::slice::Iter<'a, LoweredIndexPrefixSpec>,
    index_range_specs: std::slice::Iter<'a, LoweredIndexRangeSpec>,
}

impl<'a> AccessSpecCursor<'a> {
    /// Consume the next lowered index-prefix spec in traversal order.
    pub(in crate::db::executor) fn next_index_prefix_spec(
        &mut self,
    ) -> Option<&'a LoweredIndexPrefixSpec> {
        self.index_prefix_specs.next()
    }

    /// Consume the next lowered index-range spec in traversal order.
    pub(in crate::db::executor) fn next_index_range_spec(
        &mut self,
    ) -> Option<&'a LoweredIndexRangeSpec> {
        self.index_range_specs.next()
    }

    /// Enforce that all lowered specs were consumed during access-plan traversal.
    pub(in crate::db::executor) fn validate_consumed(&mut self) -> Result<(), InternalError> {
        if self.index_prefix_specs.next().is_some() {
            return Err(invariant(
                "unused index-prefix executable specs after access-plan traversal",
            ));
        }
        if self.index_range_specs.next().is_some() {
            return Err(invariant(
                "unused index-range executable specs after access-plan traversal",
            ));
        }

        Ok(())
    }
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
/// AccessExecutionDescriptor
///
/// Canonical runtime access descriptor for key-stream production.
/// Route/executor layers consume this descriptor instead of raw structural
/// `AccessPlan` variants.
///

pub(in crate::db::executor) struct AccessExecutionDescriptor<'a, K> {
    pub(in crate::db::executor) executable_access: ExecutableAccessPlan<'a, K>,
    pub(in crate::db::executor) bindings: AccessStreamBindings<'a>,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, K> AccessExecutionDescriptor<'a, K> {
    /// Build one canonical runtime descriptor from one structural access plan.
    #[must_use]
    pub(in crate::db::executor) fn from_bindings(
        access: &'a AccessPlan<K>,
        bindings: AccessStreamBindings<'a>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self::from_executable_bindings(
            lower_executable_access_plan(access),
            bindings,
            physical_fetch_hint,
            index_predicate_execution,
        )
    }

    /// Build one canonical runtime descriptor from one executable access plan.
    #[must_use]
    pub(in crate::db::executor) const fn from_executable_bindings(
        executable_access: ExecutableAccessPlan<'a, K>,
        bindings: AccessStreamBindings<'a>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self {
            executable_access,
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
    pub prefix: Option<&'a LoweredIndexPrefixSpec>,
    pub range: Option<&'a LoweredIndexRangeSpec>,
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
    pub physical_fetch_hint: Option<usize>,
    pub predicate_execution: Option<IndexPredicateExecution<'a>>,
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
