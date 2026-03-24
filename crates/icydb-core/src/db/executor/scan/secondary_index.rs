//! Module: executor::scan::secondary_index
//! Responsibility: secondary-index ordered fast-path stream execution helpers.
//! Does not own: planner eligibility derivation or cursor continuation semantics.
//! Boundary: consumes lowered index-prefix specs and emits ordered key streams.

use crate::{
    db::{
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutableAccess, ExecutionOptimization, LoweredIndexPrefixSpec,
            pipeline::contracts::FastPathKeyResult,
            scan::fast_stream::execute_structural_fast_stream_request,
            stream::access::StructuralTraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

/// Execute one secondary-index fast-path stream route through the structural runtime.
pub(in crate::db::executor) fn execute_secondary_index_fast_stream_route(
    runtime: &StructuralTraversalRuntime,
    plan: &AccessPlannedQuery,
    index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
    stream_direction: Direction,
    probe_fetch_hint: Option<usize>,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    // Phase 1: verify structural access-path/spec invariants for index-prefix execution.
    let access_strategy = plan.access.resolve_strategy();
    let Some(executable_path) = access_strategy.as_path() else {
        return Ok(None);
    };
    let path_capabilities = executable_path.capabilities();
    let Some(index) = path_capabilities.index_prefix_model() else {
        return Ok(None);
    };
    let Some(index_prefix_spec) = index_prefix_spec else {
        return Err(InternalError::secondary_index_prefix_spec_required());
    };
    debug_assert_eq!(
        index_prefix_spec.index(),
        &index,
        "secondary fast-path spec/index alignment must be validated by resolver",
    );

    // Phase 2: bind execution inputs and run the shared fast-stream boundary.
    let access = ExecutableAccess::from_executable_plan(
        access_strategy.into_executable(),
        AccessStreamBindings::with_index_prefix(index_prefix_spec, stream_direction),
        probe_fetch_hint,
        index_predicate_execution,
    );
    let fast = execute_structural_fast_stream_request(
        runtime,
        access,
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
