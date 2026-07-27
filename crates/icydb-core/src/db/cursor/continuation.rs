//! Module: cursor::continuation
//! Responsibility: derive bounded continuation scan and window contracts.
//! Does not own: planner continuation policy derivation or token wire schema definitions.
//! Boundary: computes runtime continuation progression under access/order/page contracts.

use crate::{
    db::{
        direction::Direction,
        index::{
            RawIndexStoreKey, resume_bounds_for_continuation,
            validate_index_scan_continuation_advancement,
        },
        query::plan::{AccessPlannedQuery, effective_offset_for_cursor_window},
    },
    error::InternalError,
};
use std::ops::Bound;

///
/// IndexScanContinuationInput
///
/// Index-scan continuation input contract for directional resume traversal.
/// Bundles optional exclusive resume anchor plus scan direction so executor
/// range traversal consumes one continuation boundary object.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexScanContinuationInput<'a> {
    anchor: Option<&'a RawIndexStoreKey>,
    direction: Direction,
}

impl<'a> IndexScanContinuationInput<'a> {
    /// Build one index-scan continuation input.
    #[must_use]
    pub(in crate::db) const fn new(
        anchor: Option<&'a RawIndexStoreKey>,
        direction: Direction,
    ) -> Self {
        Self { anchor, direction }
    }

    /// Borrow the optional raw index-key anchor carried by this scan input.
    #[must_use]
    pub(in crate::db) const fn anchor(&self) -> Option<&'a RawIndexStoreKey> {
        self.anchor
    }

    /// Borrow scan direction for continuation traversal.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Validate continuation-envelope consistency and derive resumed scan
    /// bounds for one directional index scan.
    pub(in crate::db::cursor) fn resume_bounds(
        &self,
        bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
    ) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), InternalError> {
        resume_bounds_for_continuation(self.direction, self.anchor, bounds.0, bounds.1)
    }

    /// Validate strict directional advancement for one raw-key scan candidate.
    pub(in crate::db::cursor) fn validate_candidate_advancement(
        &self,
        candidate: &RawIndexStoreKey,
    ) -> Result<(), InternalError> {
        validate_index_scan_continuation_advancement(self.direction, self.anchor, candidate)
    }
}

/// Derive the effective pagination offset for one plan under cursor-window semantics.
#[must_use]
pub(in crate::db) fn effective_page_offset_for_window(
    plan: &AccessPlannedQuery,
    cursor_boundary_present: bool,
) -> u32 {
    let window_size = plan
        .scalar_plan()
        .page
        .as_ref()
        .map_or(0, |page| page.offset);

    effective_offset_for_cursor_window(window_size, cursor_boundary_present)
}

/// Derive the effective keep-count (`offset + limit`) for one plan and limit.
#[must_use]
pub(in crate::db) fn effective_keep_count_for_limit(
    plan: &AccessPlannedQuery,
    cursor_boundary_present: bool,
    limit: u32,
) -> usize {
    let effective_offset = effective_page_offset_for_window(plan, cursor_boundary_present);

    usize::try_from(effective_offset)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
}
