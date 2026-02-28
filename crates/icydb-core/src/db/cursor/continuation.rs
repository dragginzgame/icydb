use crate::{
    db::{
        access::AccessPlan,
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, cursor_anchor_from_index_key,
            cursor_boundary_from_entity,
        },
        direction::Direction,
        index::IndexKey,
        query::plan::{OrderSpec, PageSpec},
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
        return Err(InternalError::query_executor_invariant(
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
) -> Result<ContinuationToken, InternalError>
where
    E: EntityKind + EntityValue,
{
    let boundary = cursor_boundary_from_entity(entity, order);
    let token = if let Some((index, _, _, _)) = access.as_index_range_path() {
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

// Derive the effective keep-count (`offset + limit`) under cursor-window semantics.
fn effective_keep_count_for_limit(
    page_offset: u32,
    cursor_boundary_present: bool,
    limit: u32,
) -> usize {
    let effective_offset = if cursor_boundary_present {
        0
    } else {
        page_offset
    };

    usize::try_from(effective_offset)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
}
