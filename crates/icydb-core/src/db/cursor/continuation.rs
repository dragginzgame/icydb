//! Module: cursor::continuation
//! Responsibility: derive next continuation token state from materialized/scanned rows.
//! Does not own: planner continuation policy derivation or token wire schema definitions.
//! Boundary: computes runtime continuation progression under access/order/page contracts.

use crate::{
    db::{
        access::{AccessPlan, LoweredKey},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, continuation_advanced,
            cursor_anchor_from_index_key, cursor_boundary_from_entity, resume_bounds_from_refs,
            validate_index_scan_continuation_advancement,
            validate_index_scan_continuation_envelope,
        },
        direction::Direction,
        index::{IndexKey, RawIndexKey},
        query::plan::{OrderSpec, PageSpec, effective_offset_for_cursor_window},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
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

/// Derive the next continuation token from one post-access materialized page.
#[expect(clippy::too_many_arguments)]
pub(in crate::db) fn next_cursor_for_materialized_rows<E>(
    access: &AccessPlan<E::Key>,
    order: Option<&OrderSpec>,
    page: Option<&PageSpec>,
    rows: &[(Id<E>, E)],
    rows_after_cursor: usize,
    cursor_boundary: Option<&CursorBoundary>,
    previous_index_range_anchor: Option<&LoweredKey>,
    direction: Direction,
    signature: ContinuationSignature,
) -> Result<Option<ContinuationToken>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let Some(page) = page else {
        return Ok(None);
    };
    let Some(limit) = page.limit else {
        return Ok(None);
    };
    if rows.is_empty() {
        return Ok(None);
    }

    // Continuation eligibility is computed from the post-cursor cardinality
    // against the effective page window for this request.
    let page_end = effective_keep_count_for_limit(page.offset, cursor_boundary.is_some(), limit);
    if rows_after_cursor <= page_end {
        return Ok(None);
    }

    let Some((_, last_entity)) = rows.last() else {
        return Ok(None);
    };

    let Some(order) = order else {
        return Err(InternalError::cursor_invariant(
            InternalError::executor_invariant_message(
                "cannot build continuation cursor without ordering",
            ),
        ));
    };

    next_cursor_for_entity(
        access,
        order,
        page.offset,
        last_entity,
        direction,
        signature,
        previous_index_range_anchor,
    )
    .map(Some)
}

fn next_cursor_for_entity<E>(
    access: &AccessPlan<E::Key>,
    order: &OrderSpec,
    initial_offset: u32,
    entity: &E,
    direction: Direction,
    signature: ContinuationSignature,
    previous_index_range_anchor: Option<&LoweredKey>,
) -> Result<ContinuationToken, InternalError>
where
    E: EntityKind + EntityValue,
{
    let boundary = cursor_boundary_from_entity(entity, order);
    let token = if let Some((index, _, _, _)) = access.as_index_range_path() {
        let index_key = IndexKey::new(entity, index)?.ok_or_else(|| {
            InternalError::cursor_invariant(InternalError::executor_invariant_message(
                "cursor row is not indexable for planned index-range access",
            ))
        })?;
        let last_emitted_raw_key = index_key.to_raw();
        let advanced = previous_index_range_anchor.is_none_or(|previous_anchor_raw_key| {
            continuation_advanced(direction, &last_emitted_raw_key, previous_anchor_raw_key)
        });
        if !advanced {
            return Err(InternalError::cursor_invariant(
                InternalError::executor_invariant_message(
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
            cursor_anchor_from_index_key(&index_key),
            direction,
            initial_offset,
        )
    } else {
        ContinuationToken::new_with_direction(signature, boundary, direction, initial_offset)
    };

    Ok(token)
}

// Derive the effective keep-count (`offset + limit`) under cursor-window semantics.
fn effective_keep_count_for_limit(
    page_offset: u32,
    cursor_boundary_present: bool,
    limit: u32,
) -> usize {
    let effective_offset = effective_offset_for_cursor_window(page_offset, cursor_boundary_present);

    usize::try_from(effective_offset)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
}
