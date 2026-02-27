use crate::{
    db::{
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, cursor_anchor_from_index_key,
        },
        direction::Direction,
        index::IndexKey,
        plan::{AccessPlannedQuery, effective_keep_count_for_limit},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

/// Derive the next continuation token from one post-access materialized page.
pub(in crate::db) fn next_cursor_for_materialized_rows<E>(
    plan: &AccessPlannedQuery<E::Key>,
    rows: &[(Id<E>, E)],
    rows_after_cursor: usize,
    cursor_boundary: Option<&CursorBoundary>,
    direction: Direction,
    signature: ContinuationSignature,
) -> Result<Option<ContinuationToken>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let Some(page) = plan.page.as_ref() else {
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
    let page_end = effective_keep_count_for_limit(plan, cursor_boundary.is_some(), limit);
    if rows_after_cursor <= page_end {
        return Ok(None);
    }

    let Some((_, last_entity)) = rows.last() else {
        return Ok(None);
    };

    next_cursor_for_entity(plan, last_entity, direction, signature).map(Some)
}

fn next_cursor_for_entity<E>(
    plan: &AccessPlannedQuery<E::Key>,
    entity: &E,
    direction: Direction,
    signature: ContinuationSignature,
) -> Result<ContinuationToken, InternalError>
where
    E: EntityKind + EntityValue,
{
    let boundary = plan.cursor_boundary_from_entity(entity)?;
    let initial_offset = plan.page.as_ref().map_or(0, |page| page.offset);
    let token = if let Some((index, _, _, _)) = plan.access.as_index_range_path() {
        let index_key = IndexKey::new(entity, index)?.ok_or_else(|| {
            InternalError::query_executor_invariant(
                "cursor row is not indexable for planned index-range access",
            )
        })?;

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
