//! Module: executor::load::index_range_limit
//! Responsibility: bounded index-range fast-path stream execution.
//! Does not own: index-range eligibility planning or cursor decode semantics.
//! Boundary: executes pre-lowered index-range specs when route gates allow pushdown.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessPathRuntimeStrategy, AccessStreamBindings,
            ExecutionOptimization, LoweredIndexRangeSpec, RangeToken, dispatch_access_path,
            load::{FastPathKeyResult, LoadExecutor},
            range_token_anchor_key,
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
    /// Try one bounded index-range fast-path stream for semantically safe plan shapes.
    pub(in crate::db::executor) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        index_range_anchor: Option<&RangeToken>,
        direction: Direction,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: verify access-path and executable spec materialization invariants.
        let executable_access = plan.to_executable();
        let Some(executable_path) = executable_access.as_path() else {
            return Ok(None);
        };
        let dispatched = dispatch_access_path(executable_path);
        let strategy: &dyn AccessPathRuntimeStrategy<E::Key> = dispatched;
        let Some(index) = strategy.index_range_model() else {
            return Ok(None);
        };
        let Some(index_range_spec) = index_range_spec else {
            return Err(invariant(
                "index-range executable spec must be materialized for index-range plans",
            ));
        };
        debug_assert_eq!(
            index_range_spec.index(),
            &index,
            "index-range fast-path spec/index alignment must be validated by resolver",
        );

        // Phase 2: bind range/anchor inputs and execute through shared fast-stream helper.
        let descriptor = AccessExecutionDescriptor::from_executable_bindings(
            executable_access,
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
            descriptor,
            ExecutionOptimization::IndexRangeLimitPushdown,
        )?))
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
