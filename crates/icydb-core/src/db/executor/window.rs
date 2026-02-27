use crate::{
    db::{
        cursor::{CursorBoundary, apply_cursor_boundary},
        executor::{ExecutionKernel, kernel::PlanRow},
        plan::{
            AccessPlannedQuery,
            effective_keep_count_for_limit as plan_effective_keep_count_for_limit,
            effective_page_offset_for_window,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// PageWindow
///
/// Canonical pagination window sizing in usize-domain.
/// `keep_count` is `offset + limit`, and `fetch_count` adds one extra row when requested.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageWindow {
    pub(in crate::db) fetch_count: usize,
    pub(in crate::db) keep_count: usize,
}

/// Compute canonical page window counts from logical pagination inputs.
#[must_use]
pub(in crate::db) fn compute_page_window(offset: u32, limit: u32, needs_extra: bool) -> PageWindow {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let keep_count = offset.saturating_add(limit);
    let fetch_count = keep_count.saturating_add(usize::from(needs_extra));

    PageWindow {
        fetch_count,
        keep_count,
    }
}

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
        let offset = usize::try_from(effective_page_offset_for_window(
            plan,
            cursor_boundary.is_some(),
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
        plan_effective_keep_count_for_limit(plan, cursor_boundary.is_some(), limit)
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
        effective_page_offset_for_window(plan, cursor_boundary.is_some())
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

            apply_cursor_boundary::<E, R, _>(rows, order, boundary, |row| row.entity());
            return Ok((true, rows.len()));
        }

        // No cursor boundary; preserve post-order cardinality for continuation
        // decisions and diagnostics.
        Ok((false, rows_after_order))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PageWindow, compute_page_window};

    #[test]
    fn compute_page_window_zero_offset_zero_limit_without_extra() {
        let window = compute_page_window(0, 0, false);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 0,
                keep_count: 0,
            }
        );
    }

    #[test]
    fn compute_page_window_zero_offset_zero_limit_with_extra() {
        let window = compute_page_window(0, 0, true);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 1,
                keep_count: 0,
            }
        );
    }

    #[test]
    fn compute_page_window_zero_offset_limit_one() {
        let without_extra = compute_page_window(0, 1, false);
        let with_extra = compute_page_window(0, 1, true);

        assert_eq!(
            without_extra,
            PageWindow {
                fetch_count: 1,
                keep_count: 1,
            }
        );
        assert_eq!(
            with_extra,
            PageWindow {
                fetch_count: 2,
                keep_count: 1,
            }
        );
    }

    #[test]
    fn compute_page_window_offset_n_limit_one() {
        let window = compute_page_window(37, 1, true);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 39,
                keep_count: 38,
            }
        );
    }

    #[test]
    fn compute_page_window_high_bounds_and_needs_extra_toggle() {
        let base = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
        let expected_keep = base.saturating_add(base);

        let without_extra = compute_page_window(u32::MAX, u32::MAX, false);
        let with_extra = compute_page_window(u32::MAX, u32::MAX, true);

        assert_eq!(
            without_extra,
            PageWindow {
                fetch_count: expected_keep,
                keep_count: expected_keep,
            }
        );
        assert_eq!(with_extra.keep_count, without_extra.keep_count);
        assert_eq!(
            with_extra.fetch_count,
            without_extra.fetch_count.saturating_add(1)
        );
    }
}
