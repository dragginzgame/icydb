use crate::{
    db::{
        Context,
        executor::load::{CursorPage, LoadExecutor},
        executor::{BudgetedOrderedKeyStream, OrderedKeyStream},
        query::plan::{
            ContinuationSignature, CursorBoundary, Direction, LogicalPlan, compute_page_window,
            logical::PostAccessStats,
        },
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Run the shared load phases for an already-produced ordered key stream.
    pub(super) fn materialize_key_stream_into_page(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        key_stream: &mut dyn OrderedKeyStream,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<(CursorPage<E>, usize, usize), InternalError> {
        // Apply guarded scan budgeting only when the access stream already
        // represents final canonical ordering and no residual narrowing exists.
        let data_rows = if let Some(scan_budget) = Self::derive_scan_budget(plan, cursor_boundary) {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            ctx.rows_from_ordered_key_stream(&mut budgeted, plan.consistency)?
        } else {
            ctx.rows_from_ordered_key_stream(key_stream, plan.consistency)?
        };
        let rows_scanned = data_rows.len();
        let mut rows = Context::deserialize_rows(data_rows)?;
        let page = Self::finalize_rows_into_page(
            plan,
            &mut rows,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        let post_access_rows = page.items.0.len();

        Ok((page, rows_scanned, post_access_rows))
    }

    // Derive an optional upstream scan budget for post-access pagination.
    // Returns `None` unless the plan shape is proven semantics-safe.
    fn derive_scan_budget(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> Option<usize> {
        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if !Self::is_budget_safe_shape(plan, cursor_boundary) {
            return None;
        }

        Some(compute_page_window(page.offset, limit, true).fetch_count)
    }

    // Guard scan budgeting to cases where post-access phases are pure windowing.
    fn is_budget_safe_shape(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> bool {
        if !plan.is_streaming_access_shape_safe::<E>() {
            return false;
        }

        Self::cursor_narrowing_is_budget_safe(cursor_boundary)
    }

    // Cursor boundary narrowing currently runs in post-access phases for these shapes.
    const fn cursor_narrowing_is_budget_safe(cursor_boundary: Option<&CursorBoundary>) -> bool {
        cursor_boundary.is_none()
    }

    // Apply canonical post-access phases to scanned rows and assemble the cursor page.
    fn finalize_rows_into_page(
        plan: &LogicalPlan<E::Key>,
        rows: &mut Vec<(Id<E>, E)>,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<CursorPage<E>, InternalError> {
        let stats = plan.apply_post_access_with_cursor::<E, _>(rows, cursor_boundary)?;
        let next_cursor = Self::build_next_cursor(
            plan,
            rows,
            &stats,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        let items = Response(std::mem::take(rows));

        Ok(CursorPage { items, next_cursor })
    }

    fn build_next_cursor(
        plan: &LogicalPlan<E::Key>,
        rows: &[(Id<E>, E)],
        stats: &PostAccessStats,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        signature: ContinuationSignature,
    ) -> Result<Option<Vec<u8>>, InternalError> {
        let Some(page) = plan.page.as_ref() else {
            return Ok(None);
        };
        let Some(limit) = page.limit else {
            return Ok(None);
        };
        if rows.is_empty() {
            return Ok(None);
        }

        // NOTE: post-access execution materializes full in-memory rows for Phase 1.
        let page_end =
            compute_page_window(plan.effective_page_offset(cursor_boundary), limit, false)
                .keep_count;
        if stats.rows_after_cursor <= page_end {
            return Ok(None);
        }

        let Some((_, last_entity)) = rows.last() else {
            return Ok(None);
        };

        plan.next_cursor_for_entity(last_entity, direction, signature)
            .map(Some)
    }
}
