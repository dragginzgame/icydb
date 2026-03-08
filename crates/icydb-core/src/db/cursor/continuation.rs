use crate::{
    db::{
        access::{AccessPlan, LoweredKey},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, cursor_anchor_from_index_key,
            cursor_boundary_from_entity,
        },
        direction::Direction,
        error::cursor_invariant,
        index::{IndexKey, continuation_advanced},
        query::plan::{OrderSpec, PageSpec, effective_offset_for_cursor_window},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

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
        return Err(cursor_invariant(
            "cannot build continuation cursor without ordering",
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
            cursor_invariant("cursor row is not indexable for planned index-range access")
        })?;
        let last_emitted_raw_key = index_key.to_raw();
        let advanced = previous_index_range_anchor.is_none_or(|previous_anchor_raw_key| {
            continuation_advanced(direction, &last_emitted_raw_key, previous_anchor_raw_key)
        });
        if !advanced {
            return Err(cursor_invariant(
                "index-range continuation anchor must advance strictly against previous anchor",
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
