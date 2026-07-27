//! Module: executor::window
//! Responsibility: canonical page-window and cursor-window progression helpers.
//! Does not own: access-path routing or row decoding semantics.
//! Boundary: shared pagination/cursor window calculations for executor/kernel paths.

use crate::db::{
    cursor::{CursorBoundary, effective_page_offset_for_window},
    executor::ExecutionKernel,
    query::plan::AccessPlannedQuery,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageWindow {
    fetch_count: usize,
}

fn compute_page_window(offset: u32, limit: u32) -> PageWindow {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let keep_count = offset.saturating_add(limit);

    PageWindow {
        fetch_count: keep_count.saturating_add(1),
    }
}

impl ExecutionKernel {
    /// Return the effective page offset for this request.
    #[must_use]
    pub(in crate::db::executor) fn effective_page_offset(
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> u32 {
        effective_page_offset_for_window(plan, cursor_boundary.is_some())
    }

    /// Return the bounded working-set size for ordered initial loads.
    #[must_use]
    pub(in crate::db::executor) fn bounded_order_keep_count(
        plan: &AccessPlannedQuery,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Option<usize> {
        let logical = plan.scalar_plan();
        if !logical.mode.is_load() || cursor_boundary.is_some() {
            return None;
        }

        let page = logical.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return None;
        }

        Some(compute_page_window(page.offset, limit).fetch_count)
    }
}
