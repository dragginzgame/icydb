use crate::{
    db::{
        Context,
        executor::DistinctOrderedKeyStream,
        executor::load::{
            CursorPage, ExecutionOptimization, ExecutionTrace, FastPathKeyResult, LoadExecutor,
            route::FastPathPlan,
        },
        executor::plan::set_rows_from_len,
        index::RawIndexKey,
        query::plan::{ContinuationSignature, CursorBoundary, Direction, LogicalPlan},
    },
    error::InternalError,
    obs::sink::Span,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionInputs
///
/// Shared immutable execution inputs for one load execution attempt.
/// Keeps fast-path dispatch signatures compact without changing behavior.
///

pub(super) struct ExecutionInputs<'a, E: EntityKind + EntityValue> {
    pub(super) ctx: &'a Context<'a, E>,
    pub(super) plan: &'a LogicalPlan<E::Key>,
    pub(super) cursor_boundary: Option<&'a CursorBoundary>,
    pub(super) index_range_anchor: Option<&'a RawIndexKey>,
    pub(super) direction: Direction,
    pub(super) continuation_signature: ContinuationSignature,
}

// Canonical fast-path routing decision for one execution attempt.
enum FastPathDecision {
    Pk(FastPathKeyResult),
    Secondary(FastPathKeyResult),
    IndexRange(FastPathKeyResult),
    None,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one planned fast-path route set in canonical precedence order.
    pub(super) fn try_execute_fast_path_plan(
        inputs: &ExecutionInputs<'_, E>,
        fast_path_plan: &FastPathPlan,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<Option<CursorPage<E>>, InternalError> {
        let fast = match Self::evaluate_fast_path(inputs, fast_path_plan)? {
            FastPathDecision::Pk(fast)
            | FastPathDecision::Secondary(fast)
            | FastPathDecision::IndexRange(fast) => fast,
            FastPathDecision::None => return Ok(None),
        };

        let page = Self::finalize_fast_path_page(
            inputs.ctx,
            inputs.plan,
            fast,
            inputs.cursor_boundary,
            inputs.direction,
            inputs.continuation_signature,
            span,
            execution_trace,
        )?;

        Ok(Some(page))
    }

    // Evaluate fast-path routes in canonical precedence and return one decision.
    fn evaluate_fast_path(
        inputs: &ExecutionInputs<'_, E>,
        fast_path_plan: &FastPathPlan,
    ) -> Result<FastPathDecision, InternalError> {
        if let Some(fast) = Self::try_execute_pk_order_stream(inputs.ctx, inputs.plan)? {
            return Ok(FastPathDecision::Pk(fast));
        }

        if let Some(fast) = Self::try_execute_secondary_index_order_stream(
            inputs.ctx,
            inputs.plan,
            &fast_path_plan.secondary_pushdown_applicability,
        )? {
            return Ok(FastPathDecision::Secondary(fast));
        }

        if let Some(spec) = fast_path_plan.index_range_limit_spec.as_ref()
            && let Some(fast) = Self::try_execute_index_range_limit_pushdown_stream(
                inputs.ctx,
                inputs.plan,
                inputs.index_range_anchor,
                inputs.direction,
                spec.fetch,
            )?
        {
            return Ok(FastPathDecision::IndexRange(fast));
        }

        Ok(FastPathDecision::None)
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
            super::key_stream_comparator_from_plan(plan, direction),
        )?;

        // Apply DISTINCT before post-access phases so pagination sees unique keys.
        if plan.distinct {
            key_stream = Box::new(DistinctOrderedKeyStream::new(key_stream));
        }

        let (page, keys_scanned, post_access_rows) = Self::materialize_key_stream_into_page(
            ctx,
            plan,
            key_stream.as_mut(),
            cursor_boundary,
            direction,
            continuation_signature,
        )?;

        Ok(Self::finalize_execution(
            page,
            None,
            keys_scanned,
            post_access_rows,
            span,
            execution_trace,
        ))
    }

    // Execute shared post-access materialization and observability hooks for one fast-path result.
    #[expect(
        clippy::too_many_arguments,
        reason = "fast-path finalization keeps explicit execution inputs and trace sinks"
    )]
    fn finalize_fast_path_page(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        fast: FastPathKeyResult,
        cursor_boundary: Option<&CursorBoundary>,
        direction: Direction,
        continuation_signature: ContinuationSignature,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> Result<CursorPage<E>, InternalError> {
        // Route fast-path stream output through DISTINCT when requested.
        let mut key_stream = fast.ordered_key_stream;
        if plan.distinct {
            key_stream = Box::new(DistinctOrderedKeyStream::new(key_stream));
        }

        let (page, _, post_access_rows) = Self::materialize_key_stream_into_page(
            ctx,
            plan,
            key_stream.as_mut(),
            cursor_boundary,
            direction,
            continuation_signature,
        )?;

        Ok(Self::finalize_execution(
            page,
            Some(fast.optimization),
            fast.rows_scanned,
            post_access_rows,
            span,
            execution_trace,
        ))
    }

    // Apply shared path finalization hooks after page materialization.
    fn finalize_execution(
        page: CursorPage<E>,
        optimization: Option<ExecutionOptimization>,
        rows_scanned: usize,
        post_access_rows: usize,
        span: &mut Span<E>,
        execution_trace: &mut Option<ExecutionTrace>,
    ) -> CursorPage<E> {
        Self::finalize_path_outcome(
            execution_trace,
            optimization,
            rows_scanned,
            post_access_rows,
        );
        set_rows_from_len(span, page.items.0.len());

        page
    }
}
