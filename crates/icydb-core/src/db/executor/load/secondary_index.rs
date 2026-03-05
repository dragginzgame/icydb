//! Module: executor::load::secondary_index
//! Responsibility: secondary-index ordered fast-path stream execution helpers.
//! Does not own: planner eligibility derivation or cursor continuation semantics.
//! Boundary: consumes lowered index-prefix specs and emits ordered key streams.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessStreamBindings, ExecutionOptimization,
            LoweredIndexPrefixSpec, derive_access_path_capabilities,
            load::{FastPathKeyResult, LoadExecutor},
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
    /// Try one secondary-index order fast path and return ordered keys when eligible.
    pub(in crate::db::executor) fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        // Phase 1: verify access-path/spec invariants for index-prefix execution.
        let executable_access = plan.to_executable();
        let Some(executable_path) = executable_access.as_path() else {
            return Ok(None);
        };
        let path_capabilities = derive_access_path_capabilities(executable_path);
        let Some(index) = path_capabilities.index_prefix_model() else {
            return Ok(None);
        };
        let Some(index_prefix_spec) = index_prefix_spec else {
            return Err(invariant(
                "index-prefix executable spec must be materialized for index-prefix plans",
            ));
        };
        debug_assert_eq!(
            index_prefix_spec.index(),
            &index,
            "secondary fast-path spec/index alignment must be validated by resolver",
        );
        // Phase 2: bind execution inputs and run the shared fast-stream boundary.
        let descriptor = AccessExecutionDescriptor::from_executable_bindings(
            executable_access,
            AccessStreamBindings::with_index_prefix(index_prefix_spec, stream_direction),
            probe_fetch_hint,
            index_predicate_execution,
        );

        let fast = Self::execute_fast_stream_request(
            ctx,
            descriptor,
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

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
