use crate::{
    db::{
        cursor::CursorBoundary,
        data::DataRow,
        executor::{
            ExecutionKernel, apply_structural_order_window, compare_orderable_row_with_boundary,
            projection::eval_effective_runtime_filter_program_with_value_ref_reader,
            route::access_order_satisfied_by_route_contract, terminal::page::KernelRow,
        },
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram, ResolvedOrder},
    },
    error::InternalError,
};

use super::plan::{PostAccessPredicateStrategy, PostAccessStrategy};

// Run canonical post-access phases over kernel rows.
pub(super) fn apply_post_access_to_kernel_rows_dyn(
    plan: &AccessPlannedQuery,
    rows: &mut Vec<KernelRow>,
    cursor: Option<&CursorBoundary>,
    post_access_strategy: PostAccessStrategy<'_>,
) -> Result<usize, InternalError> {
    let logical = plan.scalar_plan();

    // Phase 1: predicate filtering.
    let filtered = match post_access_strategy.predicate_strategy {
        PostAccessPredicateStrategy::NotPresent => false,
        PostAccessPredicateStrategy::AppliedDuringScan => true,
        PostAccessPredicateStrategy::Deferred { filter_program } => {
            if rows.is_empty() {
                return Ok(0);
            }

            compact_kernel_rows_in_place_result(rows, |row| {
                row_matches_filter_program(row, filter_program)
            })?;

            true
        }
    };

    // Phase 2: ordering.
    let mut ordered = false;
    let mut rows_after_order = rows.len();
    if let Some(order) = logical.order.as_ref()
        && !order.fields.is_empty()
    {
        if post_access_strategy
            .predicate_strategy
            .requires_post_access_filtering()
            && !filtered
        {
            return Err(InternalError::scalar_page_ordering_after_filtering_required());
        }

        ordered = true;
        if !access_order_satisfied_by_route_contract(plan) {
            let resolved_order = plan.require_resolved_order()?;
            let ordered_total = rows.len();

            if rows.len() > 1 {
                apply_structural_order_window(
                    rows,
                    resolved_order,
                    ExecutionKernel::bounded_order_keep_count(plan, cursor),
                );
            }
            rows_after_order = ordered_total;
        }
    }

    // Phase 3: continuation boundary.
    let rows_after_cursor = if logical.mode.is_load() {
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
        if post_access_strategy.defer_retained_slot_distinct_window {
            rows_after_order
        } else {
            let resolved_order = cursor.map(|_| plan.require_resolved_order()).transpose()?;

            apply_load_cursor_and_pagination_window(
                rows,
                cursor
                    .zip(resolved_order)
                    .map(|(boundary, resolved_order)| (resolved_order, boundary)),
                ExecutionKernel::effective_page_offset(plan, cursor),
                logical.page.as_ref().and_then(|page| page.limit),
            )
        }
    } else {
        rows_after_order
    };

    // Phase 4: apply the ordered delete window.
    if logical.mode.is_delete()
        && let Some(delete_window) = logical.delete_limit.as_ref()
    {
        if logical.order.is_some() && !ordered {
            return Err(InternalError::scalar_page_delete_limit_after_ordering_required());
        }
        apply_delete_window(rows, delete_window.offset, delete_window.limit);
    }

    Ok(rows_after_cursor)
}

// Evaluate one planner-frozen residual scalar filter program against one
// materialized kernel row.
fn row_matches_filter_program(
    row: &KernelRow,
    filter_program: &EffectiveRuntimeFilterProgram,
) -> Result<bool, InternalError> {
    eval_effective_runtime_filter_program_with_value_ref_reader(
        filter_program,
        &mut |slot| row.slot_ref(slot),
        "scalar filter expression could not read slot",
    )
}

fn apply_delete_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), offset as usize);
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), limit as usize);
        rows.truncate(limit);
    }
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

// Compact kernel rows in place under one fallible keep predicate so deferred
// residual filter evaluation can preserve runtime errors instead of treating
// them as row rejections.
fn compact_kernel_rows_in_place_result(
    rows: &mut Vec<KernelRow>,
    mut keep_row: impl FnMut(&KernelRow) -> Result<bool, InternalError>,
) -> Result<usize, InternalError> {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index])? {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    Ok(kept)
}

// Keep the test-only infallible compaction helper around so the page module
// can still pin the straight-line compaction behavior independently from the
// deferred residual-filter error path.
#[cfg(test)]
pub(crate) fn compact_kernel_rows_in_place(
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
pub(crate) fn apply_load_cursor_and_pagination_window(
    rows: &mut Vec<KernelRow>,
    cursor: Option<(&ResolvedOrder, &CursorBoundary)>,
    offset: u32,
    limit: Option<u32>,
) -> usize {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let mut kept_after_cursor = 0usize;
    let mut kept_after_page = 0usize;
    let mut limit_remaining = limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    for read_index in 0..rows.len() {
        let passes_cursor = match cursor {
            Some((resolved_order, boundary)) => {
                compare_orderable_row_with_boundary(&rows[read_index], resolved_order, boundary)
                    .is_gt()
            }
            None => true,
        };
        if !passes_cursor {
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

    kept_after_cursor
}
