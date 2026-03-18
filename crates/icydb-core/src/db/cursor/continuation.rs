//! Module: cursor::continuation
//! Responsibility: derive next continuation token state from materialized/scanned rows.
//! Does not own: planner continuation policy derivation or token wire schema definitions.
//! Boundary: computes runtime continuation progression under access/order/page contracts.

use crate::{
    db::{
        access::{AccessPlan, LoweredKey},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, continuation_advanced,
            cursor_anchor_from_raw_index_key, resume_bounds_from_refs,
            validate_index_scan_continuation_advancement,
            validate_index_scan_continuation_envelope,
        },
        direction::Direction,
        index::RawIndexKey,
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

    /// Borrow scan direction for continuation traversal.
    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Validate continuation-envelope compatibility and derive resumed scan
    /// bounds for one directional index scan.
    pub(in crate::db) fn resume_bounds(
        &self,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
    ) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), InternalError> {
        validate_index_scan_continuation_envelope(self.anchor, bounds.0, bounds.1)?;

        let resumed_bounds = match self.anchor {
            Some(anchor) => resume_bounds_from_refs(self.direction, bounds.0, bounds.1, anchor),
            None => (bounds.0.clone(), bounds.1.clone()),
        };

        Ok(resumed_bounds)
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
    last_row: Option<&MaterializedCursorRow>,
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
        return Err(crate::db::error::cursor_invariant(
            crate::db::error::executor_invariant_message(
                "cannot build continuation cursor without ordering",
            ),
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
    row: &MaterializedCursorRow,
    direction: Direction,
    signature: ContinuationSignature,
    previous_index_range_anchor: Option<&LoweredKey>,
) -> Result<ContinuationToken, InternalError> {
    let boundary = row.boundary.clone();
    let token = if let Some((_index, _, _, _)) = access.as_index_range_path() {
        let Some(last_emitted_raw_key) = row.index_anchor.as_ref() else {
            return Err(crate::db::error::cursor_invariant(
                crate::db::error::executor_invariant_message(
                    "cursor row is not indexable for planned index-range access",
                ),
            ));
        };
        let advanced = previous_index_range_anchor.is_none_or(|previous_anchor_raw_key| {
            continuation_advanced(direction, last_emitted_raw_key, previous_anchor_raw_key)
        });
        if !advanced {
            return Err(crate::db::error::cursor_invariant(
                crate::db::error::executor_invariant_message(
                    "index-range continuation anchor must advance strictly against previous anchor",
                ),
            ));
        }
        debug_assert!(
            advanced,
            "index-range continuation anchor must advance strictly against previous anchor",
        );

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
