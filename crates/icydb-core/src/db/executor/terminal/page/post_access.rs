use crate::{
    db::{
        cursor::CursorBoundary,
        data::DataRow,
        executor::{
            ExecutionKernel, PendingOrderRows, compare_orderable_row_with_boundary,
            record_rows_after_predicate, route::access_order_satisfied_by_route_mode,
            terminal::page::KernelRow,
        },
        query::plan::{AccessPlannedQuery, ResolvedOrder},
    },
    error::InternalError,
};

#[cfg(feature = "diagnostics")]
use super::metrics::{
    measure_kernel_row_phase, record_kernel_row_order_window_local_instructions,
    record_kernel_row_page_window_local_instructions,
};
// Run canonical load post-access phases over kernel rows.
pub(super) fn apply_post_access_to_kernel_rows_dyn(
    plan: &AccessPlannedQuery,
    scan_rows: PendingOrderRows<KernelRow>,
    cursor: Option<&CursorBoundary>,
    defer_retained_slot_distinct_window: bool,
) -> Result<(Vec<KernelRow>, usize), InternalError> {
    let logical = plan.scalar_plan();
    let retained_count = scan_rows.retained_count();

    // Phase 1: residual predicates are always applied while the raw row is
    // open. Post-access records the resulting cardinality but never re-runs
    // semantic filtering against a second row representation.
    record_rows_after_predicate(retained_count);

    // Phase 2: ordering.
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
            apply_measured_structural_order_window(
                scan_rows,
                resolved_order,
                ExecutionKernel::bounded_order_keep_count(plan, cursor),
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

            apply_measured_load_cursor_and_pagination_window(
                &mut rows,
                cursor
                    .zip(resolved_order)
                    .map(|(boundary, resolved_order)| (resolved_order, boundary)),
                ExecutionKernel::effective_page_offset(plan, cursor),
                logical.page.as_ref().and_then(|page| page.limit),
            )?
        }
    };

    Ok((rows, rows_after_cursor))
}

fn apply_measured_structural_order_window(
    rows: PendingOrderRows<KernelRow>,
    resolved_order: &ResolvedOrder,
    keep_count: Option<usize>,
) -> Result<Vec<KernelRow>, InternalError> {
    #[cfg(feature = "diagnostics")]
    let (order_window_local_instructions, result) =
        measure_kernel_row_phase(|| rows.apply_order(resolved_order, keep_count));
    #[cfg(feature = "diagnostics")]
    record_kernel_row_order_window_local_instructions(order_window_local_instructions);
    #[cfg(not(feature = "diagnostics"))]
    let result = rows.apply_order(resolved_order, keep_count);

    result
}

fn apply_measured_load_cursor_and_pagination_window(
    rows: &mut Vec<KernelRow>,
    cursor: Option<(&ResolvedOrder, &CursorBoundary)>,
    offset: u32,
    limit: Option<u32>,
) -> Result<usize, InternalError> {
    #[cfg(feature = "diagnostics")]
    {
        let (page_window_local_instructions, rows_after_cursor) = measure_kernel_row_phase(|| {
            apply_load_cursor_and_pagination_window(rows, cursor, offset, limit)
        });
        record_kernel_row_page_window_local_instructions(page_window_local_instructions);

        rows_after_cursor
    }

    #[cfg(not(feature = "diagnostics"))]
    apply_load_cursor_and_pagination_window(rows, cursor, offset, limit)
}

// Apply one simple cursorless load page window directly on canonical data
// rows when route order is already final and no later slot-aware phase exists.
pub(super) fn apply_data_row_page_window(plan: &AccessPlannedQuery, rows: &mut Vec<DataRow>) {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return;
    };

    let total = rows.len();
    let start = usize::try_from(page.offset)
        .unwrap_or(usize::MAX)
        .min(total);
    let end = match page.limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}

// Keep the test-only compaction helper around so the page module can pin the
// straight-line row compaction behavior independently.
#[cfg(test)]
pub(super) fn compact_kernel_rows_in_place(
    rows: &mut Vec<KernelRow>,
    mut keep_row: impl FnMut(&KernelRow) -> bool,
) -> usize {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index]) {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    kept
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
        apply_kernel_row_page_window(rows, offset, limit);

        return Ok(rows_after_cursor);
    }

    let Some((resolved_order, boundary)) = cursor else {
        return Err(InternalError::query_executor_invariant());
    };
    let mut kept_after_cursor = 0usize;
    let mut kept_after_page = 0usize;
    let mut limit_remaining = limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    for read_index in 0..rows.len() {
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

// Apply the LIMIT/OFFSET page window for the common no-cursor path without
// paying one cursor-branch check per retained row.
fn apply_kernel_row_page_window(rows: &mut Vec<KernelRow>, offset: usize, limit: Option<u32>) {
    let total = rows.len();
    let start = offset.min(total);
    let end = match limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}
