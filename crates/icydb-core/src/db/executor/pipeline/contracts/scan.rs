//! Module: executor::pipeline::contracts::scan
//! Responsibility: scan execution DTOs shared across executor entrypoints.
//! Does not own: physical stream traversal, row decoding, or route dispatch.
//! Boundary: data-only scan request shapes consumed by stream and scan runtime.

use crate::db::{
    cursor::IndexScanContinuationInput,
    direction::Direction,
    executor::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey},
};

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
/// AccessScanContinuationInput
///
/// Access-stream continuation traversal bindings.
/// Stores one index-layer continuation contract so stream/access boundaries
/// forward continuation semantics without interpreting anchor primitives.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessScanContinuationInput<'a> {
    index_scan_continuation: IndexScanContinuationInput<'a>,
}

impl<'a> AccessScanContinuationInput<'a> {
    /// Build one access-scan continuation input.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        anchor: Option<&'a LoweredKey>,
        direction: Direction,
    ) -> Self {
        Self {
            index_scan_continuation: IndexScanContinuationInput::new(anchor, direction),
        }
    }

    /// Build one initial (non-continuation) ascending scan continuation input.
    #[must_use]
    pub(in crate::db::executor) const fn initial_asc() -> Self {
        Self::new(None, Direction::Asc)
    }

    /// Borrow continuation scan direction.
    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.index_scan_continuation.direction()
    }

    /// Build one index-scan continuation contract for index-layer traversal.
    #[must_use]
    pub(in crate::db::executor) const fn index_scan_continuation(
        &self,
    ) -> IndexScanContinuationInput<'a> {
        self.index_scan_continuation
    }
}
