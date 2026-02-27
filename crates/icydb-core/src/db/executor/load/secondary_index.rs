use crate::{
    db::{
        Context,
        direction::Direction,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, KeyOrderComparator,
            LoweredIndexPrefixSpec,
            route::{RouteOrderSlotPolicy, derive_scan_direction},
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
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
    pub(in crate::db::executor) fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        let Some((index, _)) = plan.access.as_index_prefix_path() else {
            return Ok(None);
        };
        let Some(index_prefix_spec) = index_prefix_spec else {
            return Err(InternalError::query_executor_invariant(
                "index-prefix executable spec must be materialized for index-prefix plans",
            ));
        };
        debug_assert_eq!(
            index_prefix_spec.index(),
            index,
            "secondary fast-path spec/index alignment must be validated by resolver",
        );
        let stream_direction = Self::secondary_index_stream_direction(plan);

        let stream_request = AccessPlanStreamRequest {
            access: &plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: std::slice::from_ref(index_prefix_spec),
                index_range_specs: &[],
                index_range_anchor: None,
                direction: stream_direction,
            },
            key_comparator: KeyOrderComparator::from_direction(stream_direction),
            physical_fetch_hint: probe_fetch_hint,
            index_predicate_execution,
        };

        let fast = Self::execute_fast_stream_request(
            ctx,
            stream_request,
            ExecutionOptimization::SecondaryOrderPushdown,
        )?;

        if let Some(fetch) = probe_fetch_hint {
            debug_assert!(
                fast.rows_scanned <= fetch,
                "secondary fast-path rows_scanned must not exceed bounded fetch",
            );
        }

        Ok(Some(fast))
    }

    fn secondary_index_stream_direction(plan: &AccessPlannedQuery<E::Key>) -> Direction {
        derive_scan_direction(plan.order.as_ref(), RouteOrderSlotPolicy::Last)
    }
}
