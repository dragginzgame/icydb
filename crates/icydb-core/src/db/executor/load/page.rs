//! Module: executor::load::page
//! Responsibility: materialize ordered key streams into cursor-paged load rows.
//! Does not own: access-path selection, route precedence, or query planning.
//! Boundary: shared row materialization helper used by load execution paths.

use crate::{
    db::{
        Context,
        cursor::{ContinuationSignature, CursorBoundary},
        direction::Direction,
        executor::load::{CursorPage, LoadExecutor, PageCursor},
        executor::{BudgetedOrderedKeyStream, ExecutionKernel, OrderedKeyStream},
        predicate::PredicateProgram,
        query::plan::AccessPlannedQuery,
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
    /// Run shared load phases for an already-produced ordered key stream.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn materialize_key_stream_into_page(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        predicate_slots: Option<&PredicateProgram>,
        key_stream: &mut dyn OrderedKeyStream,
        scan_budget_hint: Option<usize>,
        streaming_access_shape_safe: bool,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<(CursorPage<E>, usize, usize), InternalError> {
        // Phase 1: validate scan-budget hint preconditions.
        // Bounded load scan hints are valid only for non-continuation,
        // streaming-safe access shapes where access order is final.
        if scan_budget_hint.is_some() {
            if cursor_boundary.is_some() {
                return Err(InternalError::query_executor_invariant(
                    "load page scan budget hint requires non-continuation execution",
                ));
            }
            if !streaming_access_shape_safe {
                return Err(InternalError::query_executor_invariant(
                    "load page scan budget hint requires streaming-safe access shape",
                ));
            }
        }

        // Phase 2: read rows from the ordered key stream, with optional budget guard.
        let data_rows = if let Some(scan_budget) = scan_budget_hint {
            let mut budgeted = BudgetedOrderedKeyStream::new(key_stream, scan_budget);
            ctx.rows_from_ordered_key_stream(&mut budgeted, plan.scalar_plan().consistency)?
        } else {
            ctx.rows_from_ordered_key_stream(key_stream, plan.scalar_plan().consistency)?
        };
        let rows_scanned = data_rows.len();
        let mut rows = Context::deserialize_rows(data_rows)?;

        // Phase 3: apply post-access pipeline and emit one cursor page.
        let page = Self::finalize_rows_into_page(
            plan,
            predicate_slots,
            &mut rows,
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        let post_access_rows = page.items.0.len();

        Ok((page, rows_scanned, post_access_rows))
    }

    // Apply canonical post-access phases to scanned rows and assemble the cursor page.
    fn finalize_rows_into_page(
        plan: &AccessPlannedQuery<E::Key>,
        predicate_slots: Option<&PredicateProgram>,
        rows: &mut Vec<(Id<E>, E)>,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
    ) -> Result<CursorPage<E>, InternalError> {
        let stats = ExecutionKernel::apply_post_access_with_cursor_and_compiled_predicate::<E, _, _>(
            plan,
            rows,
            cursor_boundary,
            predicate_slots,
        )?;
        let next_cursor = ExecutionKernel::next_cursor_for_materialized_rows(
            plan,
            rows,
            &stats,
            cursor_boundary,
            direction,
            continuation_signature,
        )?
        .map(PageCursor::Scalar);
        let items = Response(std::mem::take(rows));

        Ok(CursorPage { items, next_cursor })
    }
}
