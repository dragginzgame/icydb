use crate::db::{
    cursor::CursorBoundary,
    executor::{
        ExecutionKernel, compute_page_window,
        kernel::{PlanRow, post_access::order_cursor},
    },
    plan::AccessPlannedQuery,
};
use crate::{
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// WindowCursorContract
///
/// WindowCursorContract tracks effective offset/limit progression under the
/// canonical cursor-aware window policy owned by the execution kernel.
///

pub(in crate::db::executor) struct WindowCursorContract {
    offset_remaining: usize,
    limit_remaining: Option<usize>,
}

impl WindowCursorContract {
    // Build one kernel-owned window contract from canonical effective offset.
    fn from_plan<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Self {
        let offset = usize::try_from(ExecutionKernel::effective_page_offset(
            plan,
            cursor_boundary,
        ))
        .unwrap_or(usize::MAX);
        let limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        Self {
            offset_remaining: offset,
            limit_remaining: limit,
        }
    }

    pub(in crate::db::executor) const fn exhausted(&self) -> bool {
        matches!(self.limit_remaining, Some(0))
    }

    // Advance window state by one existing row and return whether the row is
    // in the effective output window.
    pub(in crate::db::executor) const fn accept_existing_row(&mut self) -> bool {
        if self.offset_remaining > 0 {
            self.offset_remaining = self.offset_remaining.saturating_sub(1);
            return false;
        }

        if let Some(remaining) = self.limit_remaining.as_mut() {
            if *remaining == 0 {
                return false;
            }

            *remaining = remaining.saturating_sub(1);
        }

        true
    }
}

impl ExecutionKernel {
    // Build one kernel-owned window/cursor progression contract for stream
    // reducer execution.
    pub(in crate::db::executor) fn window_cursor_contract<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> WindowCursorContract {
        WindowCursorContract::from_plan(plan, cursor_boundary)
    }

    // Compute one kernel-owned effective keep-count from plan + cursor offset
    // semantics for pagination/retry boundaries.
    pub(in crate::db::executor) fn effective_keep_count_for_limit<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
        limit: u32,
    ) -> usize {
        compute_page_window(
            Self::effective_page_offset(plan, cursor_boundary),
            limit,
            false,
        )
        .keep_count
    }

    /// Return the effective page offset for this request.
    ///
    /// Offset is only consumed on the first page. Any continuation cursor means
    /// the offset has already been applied and the next request uses offset `0`.
    #[must_use]
    pub(in crate::db::executor) fn effective_page_offset<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> u32 {
        if cursor_boundary.is_some() {
            return 0;
        }

        plan.page.as_ref().map_or(0, |page| page.offset)
    }

    // Return the bounded working-set size for ordered loads without a
    // continuation boundary. This keeps canonical semantics while avoiding a
    // full sort when only one page window (+1 to detect continuation) is
    // needed.
    #[must_use]
    pub(in crate::db::executor) fn bounded_order_keep_count<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Option<usize> {
        if !plan.mode.is_load() || cursor_boundary.is_some() {
            return None;
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return None;
        }

        Some(compute_page_window(page.offset, limit, true).fetch_count)
    }

    // Continuation phase (strictly after ordering, before pagination).
    pub(in crate::db::executor) fn apply_cursor_boundary_phase<K, E, R>(
        plan: &AccessPlannedQuery<K>,
        rows: &mut Vec<R>,
        cursor_boundary: Option<&CursorBoundary>,
        ordered: bool,
        rows_after_order: usize,
    ) -> Result<(bool, usize), InternalError>
    where
        E: EntityKind + EntityValue,
        R: PlanRow<E>,
    {
        if plan.mode.is_load()
            && let Some(boundary) = cursor_boundary
        {
            let Some(order) = plan.order.as_ref() else {
                return Err(InternalError::query_executor_invariant(
                    "cursor boundary requires ordering",
                ));
            };

            if !ordered {
                return Err(InternalError::query_executor_invariant(
                    "cursor boundary must run after ordering",
                ));
            }

            order_cursor::apply_cursor_boundary::<E, R>(rows, order, boundary);
            return Ok((true, rows.len()));
        }

        // No cursor boundary; preserve post-order cardinality for continuation
        // decisions and diagnostics.
        Ok((false, rows_after_order))
    }
}
