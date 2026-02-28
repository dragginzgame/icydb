use crate::{
    db::{
        Context,
        executor::load::{ExecutionOptimization, FastPathKeyResult, LoadExecutor},
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, LoweredIndexPrefixSpec,
            traversal::derive_secondary_order_scan_direction,
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
        let stream_direction =
            derive_secondary_order_scan_direction(plan.scalar_plan().order.as_ref());

        let stream_request = AccessPlanStreamRequest::from_bindings(
            &plan.access,
            AccessStreamBindings::with_index_prefix(index_prefix_spec, stream_direction),
            probe_fetch_hint,
            index_predicate_execution,
        );

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
}
