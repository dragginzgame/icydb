use crate::{
    db::{
        Context,
        executor::VecOrderedKeyStream,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        index::RawIndexKey,
        query::plan::{Direction, IndexRangeSpec, LogicalPlan},
        query::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Limited IndexRange pushdown for semantically safe plan shapes.
    pub(super) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        index_range_spec: Option<&IndexRangeSpec>,
        index_range_anchor: Option<&RawIndexKey>,
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
        if index_range_spec.index() != index {
            return Err(InternalError::query_executor_invariant(
                "index-range spec does not match access path index",
            ));
        }

        // Phase 1: resolve candidate keys via bounded range traversal with early termination.
        let ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index_range_spec.index().store)
                .and_then(|store| {
                    store.with_index(|index_store| {
                        index_store.resolve_data_values_in_raw_range_limited::<E>(
                            index_range_spec.index(),
                            (index_range_spec.lower(), index_range_spec.upper()),
                            index_range_anchor,
                            direction,
                            effective_fetch,
                            index_predicate_execution,
                        )
                    })
                })
        })?;
        let rows_scanned = ordered_keys.len();

        Ok(Some(FastPathKeyResult {
            ordered_key_stream: Box::new(VecOrderedKeyStream::new(ordered_keys)),
            rows_scanned,
            optimization: ExecutionOptimization::IndexRangeLimitPushdown,
        }))
    }
}
