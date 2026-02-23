use crate::{
    db::{
        Context,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{VecOrderedKeyStream, normalize_ordered_keys},
        query::plan::{
            Direction, IndexPrefixSpec, LogicalPlan, SlotSelectionPolicy, derive_scan_direction,
        },
        query::predicate::IndexPredicateProgram,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Fast path for secondary-index traversal when planner pushdown eligibility
    // proves canonical ORDER BY parity with raw index-key order.
    pub(super) fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        index_prefix_spec: Option<&IndexPrefixSpec>,
        probe_fetch_hint: Option<usize>,
        index_predicate_program: Option<&IndexPredicateProgram>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        let Some((index, _)) = plan.access.as_index_prefix_path() else {
            return Ok(None);
        };
        let Some(index_prefix_spec) = index_prefix_spec else {
            return Err(InternalError::query_executor_invariant(
                "index-prefix executable spec must be materialized for index-prefix plans",
            ));
        };
        if index_prefix_spec.index() != index {
            return Err(InternalError::query_executor_invariant(
                "index-prefix spec does not match access path index",
            ));
        }
        let stream_direction = Self::secondary_index_stream_direction(plan);

        // Phase 1: resolve candidate keys using canonical index traversal order.
        // When a probe hint is present (EXISTS), use a bounded resolver so
        // candidate production can short-circuit earlier.
        let mut ordered_keys = ctx.db.with_store_registry(|reg| {
            reg.try_get_store(index_prefix_spec.index().store)
                .and_then(|store| {
                    store.with_index(|index_store| match probe_fetch_hint {
                        Some(fetch) => index_store.resolve_data_values_in_raw_range_limited::<E>(
                            index_prefix_spec.index(),
                            (index_prefix_spec.lower(), index_prefix_spec.upper()),
                            None,
                            stream_direction,
                            fetch,
                            index_predicate_program,
                        ),
                        None => index_store.resolve_data_values_in_raw_range_limited::<E>(
                            index_prefix_spec.index(),
                            (index_prefix_spec.lower(), index_prefix_spec.upper()),
                            None,
                            stream_direction,
                            usize::MAX,
                            index_predicate_program,
                        ),
                    })
                })
        })?;

        // The bounded resolver already returns keys in requested order.
        if probe_fetch_hint.is_none() {
            normalize_ordered_keys(&mut ordered_keys, stream_direction, true);
        }
        let rows_scanned = ordered_keys.len();

        Ok(Some(FastPathKeyResult {
            ordered_key_stream: Box::new(VecOrderedKeyStream::new(ordered_keys)),
            rows_scanned,
            optimization: ExecutionOptimization::SecondaryOrderPushdown,
        }))
    }

    fn secondary_index_stream_direction(plan: &LogicalPlan<E::Key>) -> Direction {
        plan.order.as_ref().map_or(Direction::Asc, |order| {
            derive_scan_direction(order, SlotSelectionPolicy::Last)
        })
    }
}
