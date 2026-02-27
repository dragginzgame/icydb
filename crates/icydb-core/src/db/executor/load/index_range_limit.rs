use crate::{
    db::{
        Context,
        direction::Direction,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, LoweredIndexRangeSpec, RangeToken,
            range_token_anchor_key,
        },
        index::predicate::IndexPredicateExecution,
        plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Limited IndexRange pushdown for semantically safe plan shapes.
    pub(in crate::db::executor) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        index_range_anchor: Option<&RangeToken>,
        direction: Direction,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        let Some((index, _, _, _)) = plan.access.as_index_range_path() else {
            return Ok(None);
        };
        let Some(index_range_spec) = index_range_spec else {
            return Err(InternalError::query_executor_invariant(
                "index-range executable spec must be materialized for index-range plans",
            ));
        };
        debug_assert_eq!(
            index_range_spec.index(),
            index,
            "index-range fast-path spec/index alignment must be validated by resolver",
        );
        let stream_request = AccessPlanStreamRequest::from_bindings(
            &plan.access,
            AccessStreamBindings::with_index_range(
                index_range_spec,
                index_range_anchor.map(range_token_anchor_key),
                direction,
            ),
            Some(effective_fetch),
            index_predicate_execution,
        );

        Ok(Some(Self::execute_fast_stream_request(
            ctx,
            stream_request,
            ExecutionOptimization::IndexRangeLimitPushdown,
        )?))
    }
}
