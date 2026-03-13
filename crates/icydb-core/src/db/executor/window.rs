//! Module: executor::window
//! Responsibility: canonical page-window and cursor-window progression helpers.
//! Does not own: access-path routing or row decoding semantics.
//! Boundary: shared pagination/cursor window calculations for executor/kernel paths.

use crate::{
    db::{
        cursor::{
            CursorBoundary, WindowCursorContract, apply_resume_bound_phase,
            effective_page_offset_for_window, window_cursor_contract_for_plan,
        },
        executor::{ExecutionKernel, PlanRow},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// PageWindow
///
/// Canonical pagination window sizing in usize-domain.
/// `keep_count` is `offset + limit`, and `fetch_count` always includes one
/// lookahead row for continuation detection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct PageWindow {
    pub(in crate::db) fetch_count: usize,
    pub(in crate::db) keep_count: usize,
}

/// Compute canonical keep-count from logical pagination inputs.
#[must_use]
pub(in crate::db) fn compute_page_keep_count(offset: u32, limit: u32) -> usize {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    offset.saturating_add(limit)
}

/// Compute canonical page window counts from logical pagination inputs.
#[must_use]
pub(in crate::db) fn compute_page_window(offset: u32, limit: u32) -> PageWindow {
    let keep_count = compute_page_keep_count(offset, limit);
    let fetch_count = keep_count.saturating_add(1);

    PageWindow {
        fetch_count,
        keep_count,
    }
}

/// Compute canonical `(keep_count, fetch_count)` for one pagination window.
///
/// Callers that need both values should use this helper to avoid duplicated
/// offset/limit arithmetic and independent window projections.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn compute_page_keep_and_fetch_counts(offset: u32, limit: u32) -> (usize, usize) {
    let window = compute_page_window(offset, limit);
    (window.keep_count, window.fetch_count)
}

impl ExecutionKernel {
    /// Build one kernel-owned window/cursor progression contract for stream reducers.
    pub(in crate::db::executor) fn window_cursor_contract<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> WindowCursorContract {
        window_cursor_contract_for_plan(plan, cursor_boundary)
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

    /// Apply continuation-boundary phase after ordering and before pagination.
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
        apply_resume_bound_phase::<K, E, R, _>(
            plan,
            rows,
            cursor_boundary,
            ordered,
            rows_after_order,
            |row| row.entity(),
        )
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        PageWindow, compute_page_keep_and_fetch_counts, compute_page_keep_count,
        compute_page_window,
    };

    #[test]
    fn compute_page_window_zero_offset_zero_limit_projects_keep_and_fetch() {
        let window = compute_page_window(0, 0);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 1,
                keep_count: 0,
            }
        );
    }

    #[test]
    fn compute_page_window_zero_offset_limit_one_projects_keep_and_fetch() {
        let window = compute_page_window(0, 1);

        assert_eq!(
            window,
            PageWindow {
                fetch_count: 2,
                keep_count: 1,
            }
        );
    }

    #[test]
    fn compute_page_window_offset_n_limit_one() {
        let window = compute_page_window(37, 1);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 39,
                keep_count: 38,
            }
        );
    }

    #[test]
    fn compute_page_window_high_bounds_saturates_keep_and_fetch() {
        let base = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
        let expected_keep = base.saturating_add(base);

        let window = compute_page_window(u32::MAX, u32::MAX);

        assert_eq!(
            window,
            PageWindow {
                fetch_count: expected_keep.saturating_add(1),
                keep_count: expected_keep,
            }
        );
    }

    #[test]
    fn compute_page_keep_and_fetch_counts_matches_window_projections() {
        let (keep_count, fetch_count) = compute_page_keep_and_fetch_counts(37, 11);

        assert_eq!(keep_count, compute_page_keep_count(37, 11));
        assert_eq!(fetch_count, compute_page_window(37, 11).fetch_count);
    }
}
