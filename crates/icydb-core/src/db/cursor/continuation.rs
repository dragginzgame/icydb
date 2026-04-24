//! Module: cursor::continuation
//! Responsibility: derive next continuation token state from materialized/scanned rows.
//! Does not own: planner continuation policy derivation or token wire schema definitions.
//! Boundary: computes runtime continuation progression under access/order/page contracts.

use crate::{
    db::{
        access::{AccessPlan, LoweredKey},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary,
            cursor_anchor_from_raw_index_key,
        },
        direction::Direction,
        index::{
            RawIndexKey, resume_bounds_for_continuation,
            validate_index_scan_continuation_advancement,
        },
        query::plan::{
            AccessPlannedQuery, OrderSpec, PageSpec, effective_offset_for_cursor_window,
        },
    },
    error::InternalError,
};
use std::ops::Bound;

///
/// IndexScanContinuationInput
///
/// Index-scan continuation input contract for directional resume traversal.
/// Bundles optional exclusive resume anchor plus scan direction so scan-layer
/// range traversal consumes one continuation boundary object.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexScanContinuationInput<'a> {
    anchor: Option<&'a RawIndexKey>,
    direction: Direction,
}

impl<'a> IndexScanContinuationInput<'a> {
    /// Build one index-scan continuation input.
    #[must_use]
    pub(in crate::db) const fn new(anchor: Option<&'a RawIndexKey>, direction: Direction) -> Self {
        Self { anchor, direction }
    }

    /// Return whether this scan is resuming from one validated anchor.
    #[must_use]
    pub(in crate::db) const fn has_anchor(&self) -> bool {
        self.anchor.is_some()
    }

    /// Borrow scan direction for continuation traversal.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Validate continuation-envelope consistency and derive resumed scan
    /// bounds for one directional index scan.
    pub(in crate::db) fn resume_bounds(
        &self,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    ) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), InternalError> {
        resume_bounds_for_continuation(self.direction, self.anchor, bounds.0, bounds.1)
    }

    /// Validate strict directional advancement for one raw-key scan candidate.
    pub(in crate::db) fn validate_candidate_advancement(
        &self,
        candidate: &RawIndexKey,
    ) -> Result<(), InternalError> {
        validate_index_scan_continuation_advancement(self.direction, self.anchor, candidate)
    }
}

///
/// MaterializedCursorRow
///
/// Structural continuation-row envelope produced after post-access ordering and
/// cursor filtering. Carries the exact boundary payload and optional index-range
/// anchor key needed to mint the next continuation token without typed entities.
///
#[derive(Clone, Debug)]
pub(in crate::db) struct MaterializedCursorRow {
    boundary: CursorBoundary,
    index_anchor: Option<RawIndexKey>,
}

impl MaterializedCursorRow {
    /// Build one structural continuation row from resolved boundary data.
    #[must_use]
    pub(in crate::db) const fn new(
        boundary: CursorBoundary,
        index_anchor: Option<RawIndexKey>,
    ) -> Self {
        Self {
            boundary,
            index_anchor,
        }
    }
}

/// Derive the next continuation token from one post-access materialized page.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn next_cursor_for_materialized_rows<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    page: Option<&PageSpec>,
    rows_len: usize,
    last_row: Option<MaterializedCursorRow>,
    rows_after_cursor: usize,
    cursor_boundary: Option<&CursorBoundary>,
    previous_index_range_anchor: Option<&LoweredKey>,
    direction: Direction,
    signature: ContinuationSignature,
) -> Result<Option<ContinuationToken>, InternalError> {
    let Some(page) = page else {
        return Ok(None);
    };
    let Some(limit) = page.limit else {
        return Ok(None);
    };
    if rows_len == 0 {
        return Ok(None);
    }

    // Continuation eligibility is computed from the post-cursor cardinality
    // against the effective page window for this request.
    let page_end =
        effective_keep_count_for_page_limit(page.offset, cursor_boundary.is_some(), limit);
    if rows_after_cursor <= page_end {
        return Ok(None);
    }

    let Some(last_row) = last_row else {
        return Ok(None);
    };

    let Some(_order) = order else {
        return Err(InternalError::cursor_executor_invariant(
            "cannot build continuation cursor without ordering",
        ));
    };

    next_cursor_for_row(
        access,
        page.offset,
        last_row,
        direction,
        signature,
        previous_index_range_anchor,
    )
    .map(Some)
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

fn next_cursor_for_row<K>(
    access: &AccessPlan<K>,
    initial_offset: u32,
    row: MaterializedCursorRow,
    direction: Direction,
    signature: ContinuationSignature,
    previous_index_range_anchor: Option<&LoweredKey>,
) -> Result<ContinuationToken, InternalError> {
    let MaterializedCursorRow {
        boundary,
        index_anchor,
    } = row;

    let token = if access.as_index_range_path().is_some() {
        let Some(last_emitted_raw_key) = index_anchor.as_ref() else {
            return Err(InternalError::cursor_executor_invariant(
                "cursor row is not indexable for planned index-range access",
            ));
        };
        validate_next_index_range_anchor_progression(
            direction,
            previous_index_range_anchor,
            last_emitted_raw_key,
        )?;

        ContinuationToken::new_index_range_with_direction(
            signature,
            boundary,
            cursor_anchor_from_raw_index_key(last_emitted_raw_key),
            direction,
            initial_offset,
        )
    } else {
        ContinuationToken::new_with_direction(signature, boundary, direction, initial_offset)
    };

    Ok(token)
}

fn validate_next_index_range_anchor_progression(
    direction: Direction,
    previous_anchor: Option<&LoweredKey>,
    last_emitted_raw_key: &RawIndexKey,
) -> Result<(), InternalError> {
    validate_index_scan_continuation_advancement(direction, previous_anchor, last_emitted_raw_key)
        .map_err(|_| {
            InternalError::cursor_executor_invariant(
                "index-range continuation anchor must advance strictly against previous anchor",
            )
        })?;

    debug_assert!(
        previous_anchor.is_none_or(
            |previous_anchor| validate_index_scan_continuation_advancement(
                direction,
                Some(previous_anchor),
                last_emitted_raw_key,
            )
            .is_ok()
        ),
        "index-range continuation anchor must advance strictly against previous anchor",
    );

    Ok(())
}

// Derive the effective keep-count (`offset + limit`) under cursor-window semantics.
fn effective_keep_count_for_page_limit(
    page_offset: u32,
    cursor_boundary_present: bool,
    limit: u32,
) -> usize {
    let effective_offset = effective_offset_for_cursor_window(page_offset, cursor_boundary_present);

    usize::try_from(effective_offset)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
}
