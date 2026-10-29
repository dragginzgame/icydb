use crate::{
    db::{
        cursor::CursorBoundary,
        executor::{
            ExecutionKernel, PendingOrderRows, ProjectionDataRows,
            compare_orderable_row_with_boundary, pipeline::contracts::CursorEmissionMode,
            route::access_order_satisfied_by_route_mode, terminal::page::KernelRow,
        },
        query::plan::{AccessPlannedQuery, ResolvedOrder},
        resource::budget::charge_current_execution_budget,
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;

// Run canonical load post-access phases over kernel rows.
pub(super) fn apply_post_access_to_kernel_rows_dyn(
    plan: &AccessPlannedQuery,
    scan_rows: PendingOrderRows<KernelRow>,
    cursor: Option<&CursorBoundary>,
    cursor_emission: CursorEmissionMode,
    defer_retained_slot_distinct_window: bool,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let logical = plan.scalar_plan();
    let retained_count = scan_rows.retained_count();

    // Phase 1: ordering.
    let ordered = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    let rows_after_order = retained_count;
    let mut rows = if ordered {
        if access_order_satisfied_by_route_mode(plan) {
            scan_rows.into_plain_rows()?
        } else {
            let resolved_order = plan.require_resolved_order()?;
            if scan_rows
                .plain_rows()
                .is_some_and(|rows| rows.iter().any(|row| !row.has_materialized_slots()))
            {
                return Err(InternalError::query_executor_invariant());
            }
            scan_rows.apply_order(
                resolved_order,
                ExecutionKernel::bounded_order_keep_count(plan, cursor, cursor_emission.enabled()),
            )?
        }
    } else {
        scan_rows.into_plain_rows()?
    };

    // Phase 3: continuation boundary.
    let rows_after_cursor = {
        if cursor.is_some() {
            if logical.order.is_none() {
                return Err(InternalError::scalar_page_cursor_boundary_order_required());
            }
            if !ordered {
                return Err(InternalError::scalar_page_cursor_boundary_after_ordering_required());
            }
        }
        if logical
            .page
            .as_ref()
            .is_some_and(|_| logical.order.is_some() && !ordered)
        {
            return Err(InternalError::scalar_page_pagination_after_ordering_required());
        }
        if defer_retained_slot_distinct_window {
            rows_after_order
        } else {
            let resolved_order = cursor.map(|_| plan.require_resolved_order()).transpose()?;

            apply_load_cursor_and_pagination_window(
                &mut rows,
                cursor
                    .zip(resolved_order)
                    .map(|(boundary, resolved_order)| (resolved_order, boundary)),
                ExecutionKernel::effective_page_offset(plan, cursor),
                // Preserve the scanner's lookahead until projection decides
                // continuation; only returned rows consume the output envelope.
                logical
                    .page
                    .as_ref()
                    .and_then(|page| page.limit)
                    .map(|limit| {
                        limit.saturating_add(u32::from(cursor_emission.enabled() && limit > 0))
                    }),
            )?
        }
    };

    Ok((rows, rows_after_cursor))
}

// Apply one simple cursorless load page window directly on canonical data
// rows when route order is already final and no later slot-aware phase exists.
pub(super) fn apply_data_row_page_window(
    plan: &AccessPlannedQuery,
    rows: &mut ProjectionDataRows,
) -> Result<(), InternalError> {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return Ok(());
    };

    let total = rows.row_count();
    apply_compact_page_window(
        rows,
        total,
        usize::try_from(page.offset).unwrap_or(usize::MAX),
        page.limit,
        ProjectionDataRows::swap,
        ProjectionDataRows::truncate,
    )
}

// Apply the ordered-load continuation boundary and page window in one in-place
// compaction pass so rows do not go through separate retain, drain, and
// truncate passes after materialization.
pub(super) fn apply_load_cursor_and_pagination_window(
    rows: &mut Vec<KernelRow>,
    cursor: Option<(&ResolvedOrder, &CursorBoundary)>,
    offset: u32,
    limit: Option<u32>,
) -> Result<usize, InternalError> {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    if cursor.is_none() {
        let rows_after_cursor = rows.len();
        apply_compact_page_window(
            rows,
            rows_after_cursor,
            offset,
            limit,
            |rows, left, right| {
                rows.swap(left, right);
                Ok(())
            },
            Vec::truncate,
        )?;

        return Ok(rows_after_cursor);
    }

    let Some((resolved_order, boundary)) = cursor else {
        return Err(InternalError::query_executor_invariant());
    };
    let mut kept_after_cursor = 0usize;
    let mut kept_after_page = 0usize;
    let mut limit_remaining = limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    for read_index in 0..rows.len() {
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
        if !compare_orderable_row_with_boundary(&rows[read_index], resolved_order, boundary)?
            .is_gt()
        {
            continue;
        }

        kept_after_cursor = kept_after_cursor.saturating_add(1);
        if kept_after_cursor <= offset {
            continue;
        }
        if limit_remaining.is_some_and(|remaining| remaining == 0) {
            continue;
        }

        if let Some(remaining) = limit_remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
        }

        if kept_after_page != read_index {
            rows.swap(kept_after_page, read_index);
        }
        kept_after_page = kept_after_page.saturating_add(1);
    }

    rows.truncate(kept_after_page);

    Ok(kept_after_cursor)
}

// Compact either row container with the same window and movement policy.
// Container operations stay explicit: canonical data-row swaps are fallible,
// while kernel vectors swap directly. Neither path allocates another row list.
fn apply_compact_page_window<R>(
    rows: &mut R,
    total: usize,
    offset: usize,
    limit: Option<u32>,
    mut swap: impl FnMut(&mut R, usize, usize) -> Result<(), InternalError>,
    truncate: impl FnOnce(&mut R, usize),
) -> Result<(), InternalError> {
    let start = offset.min(total);
    let end = match limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        truncate(rows, end);
        return Ok(());
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            swap(rows, kept, read_index)?;
        }
        kept = kept.saturating_add(1);
    }

    truncate(rows, kept);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::apply_compact_page_window;
    use crate::{db::executor::ProjectionDataRows, error::InternalError};

    #[test]
    fn compact_page_windows_preserve_order_and_zero_offset_needs_no_swaps() {
        for total in 0..=6 {
            for offset in [0, 1, 3, 6, usize::MAX] {
                for limit in [None, Some(0), Some(1), Some(4), Some(u32::MAX)] {
                    let mut rows = (0..total).collect::<Vec<_>>();
                    let expected = rows
                        .iter()
                        .copied()
                        .skip(offset)
                        .take(limit.map_or(usize::MAX, |limit| usize::try_from(limit).unwrap()))
                        .collect::<Vec<_>>();
                    let mut swaps = 0;
                    apply_compact_page_window(
                        &mut rows,
                        total,
                        offset,
                        limit,
                        |rows, left, right| {
                            swaps += 1;
                            rows.swap(left, right);
                            Ok(())
                        },
                        Vec::truncate,
                    )
                    .unwrap();
                    assert_eq!(rows, expected);
                    if offset == 0 {
                        assert_eq!(swaps, 0);
                    }
                }
            }
        }
    }

    #[test]
    fn compact_page_window_propagates_data_row_swap_refusal_without_truncation() {
        let mut rows = ProjectionDataRows::raw(Vec::new());
        let error = apply_compact_page_window(
            &mut rows,
            2,
            1,
            Some(1),
            ProjectionDataRows::swap,
            |_, _| panic!("failed compaction must not truncate"),
        )
        .unwrap_err();
        assert_eq!(
            error.diagnostic_code(),
            InternalError::query_executor_invariant().diagnostic_code()
        );
    }
}
