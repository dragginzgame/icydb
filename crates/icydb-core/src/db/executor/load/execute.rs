use crate::{
    db::{
        Context,
        executor::load::{
            CursorPage, ExecutionTrace, FastPathKeyResult, LoadExecutor, route::FastPathPlan,
        },
        executor::plan::set_rows_from_len,
        index::RawIndexKey,
        query::plan::{ContinuationSignature, CursorBoundary, Direction, LogicalPlan},
    },
    error::InternalError,
    obs::sink::Span,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one planned fast-path route set in canonical precedence order.
    #[expect(
        clippy::too_many_arguments,
        reason = "fast-path dispatch keeps execution inputs explicit at one call site"
    )]
    pub(super) fn try_execute_fast_path_plan(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        fast_path_plan: &FastPathPlan,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<Option<CursorPage<E>>, InternalError> {
        if let Some(fast) = Self::try_execute_pk_order_stream(ctx, plan)? {
            let page = Self::finalize_fast_path_page(
                ctx,
                plan,
                fast,
                cursor_boundary,
                direction,
                continuation_signature,
                span,
                execution_trace,
            )?;

            return Ok(Some(page));
        }

        if let Some(fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            plan,
            &fast_path_plan.secondary_pushdown_applicability,
        )? {
            let page = Self::finalize_fast_path_page(
                ctx,
                plan,
                fast,
                cursor_boundary,
                direction,
                continuation_signature,
                span,
                execution_trace,
            )?;

            return Ok(Some(page));
        }

        if let Some(spec) = fast_path_plan.index_range_limit_spec.as_ref()
            && let Some(fast) = Self::try_execute_index_range_limit_pushdown_stream(
                ctx,
                plan,
                index_range_anchor,
                direction,
                spec.fetch,
            )?
        {
            let page = Self::finalize_fast_path_page(
                ctx,
                plan,
                fast,
                cursor_boundary,
                direction,
                continuation_signature,
                span,
                execution_trace,
            )?;

            return Ok(Some(page));
        }

        Ok(None)
    }

    // Execute canonical fallback stream production + shared materialization phases.
    #[expect(
        clippy::too_many_arguments,
        reason = "fallback execution keeps load inputs explicit at one boundary"
    )]
    pub(super) fn execute_fallback_path(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<CursorPage<E>, InternalError> {
        let mut key_stream = ctx.ordered_key_stream_from_access_plan_with_index_range_anchor(
            &plan.access,
            index_range_anchor,
            direction,
        )?;
        let (page, keys_scanned, post_access_rows) = Self::materialize_key_stream_into_page(
            ctx,
            plan,
            key_stream.as_mut(),
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        Self::finalize_path_outcome(execution_trace, None, keys_scanned, post_access_rows);
        set_rows_from_len(span, page.items.0.len());

        Ok(page)
    }

    // Execute shared post-access materialization and observability hooks for one fast-path result.
    #[expect(
        clippy::too_many_arguments,
        reason = "fast-path finalization keeps explicit execution inputs and trace sinks"
    )]
    fn finalize_fast_path_page(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        mut fast: FastPathKeyResult,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<CursorPage<E>, InternalError> {
        let (page, _, post_access_rows) = Self::materialize_key_stream_into_page(
            ctx,
            plan,
            fast.ordered_key_stream.as_mut(),
            cursor_boundary,
            direction,
            continuation_signature,
        )?;
        Self::finalize_path_outcome(
            execution_trace,
            Some(fast.optimization),
            fast.rows_scanned,
            post_access_rows,
        );
        set_rows_from_len(span, page.items.0.len());

        Ok(page)
    }
}
