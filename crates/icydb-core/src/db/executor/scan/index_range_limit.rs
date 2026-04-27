//! Module: executor::scan::index_range_limit
//! Responsibility: bounded index-range fast-path stream execution.
//! Does not own: index-range eligibility planning or cursor decode semantics.
//! Boundary: executes pre-lowered index-range specs when route gates allow pushdown.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionOptimization,
            LoweredIndexRangeSpec, pipeline::contracts::FastPathKeyResult,
            scan::fast_stream::execute_structural_fast_stream_request,
            stream::access::TraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};

/// Execute one bounded index-range fast-path stream through the structural runtime.
pub(in crate::db::executor) fn execute_index_range_fast_stream_route(
    runtime: &TraversalRuntime,
    _plan: &AccessPlannedQuery,
    executable: &ExecutableAccessPlan<'_, Value>,
    index_range_spec: Option<&LoweredIndexRangeSpec>,
    continuation: AccessScanContinuationInput<'_>,
    effective_fetch: usize,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    // Phase 1: verify structural access-path and executable spec materialization invariants.
    let Some(executable_path) = executable.as_path() else {
        return Ok(None);
    };
    let path_capabilities = executable_path.capabilities();
    let Some(details) = path_capabilities.index_range_details() else {
        return Ok(None);
    };
    let index = details.index();
    let Some(index_range_spec) = index_range_spec else {
        return Err(InternalError::index_range_limit_spec_required());
    };
    debug_assert_eq!(
        index_range_spec.index(),
        &index,
        "index-range fast-path spec/index alignment must be validated by resolver",
    );

    // Phase 2: bind range/anchor inputs and execute through the shared fast-stream helper.
    Ok(Some(execute_structural_fast_stream_request(
        runtime,
        executable,
        AccessStreamBindings::with_index_range_continuation(index_range_spec, continuation),
        Some(effective_fetch),
        index_predicate_execution,
        ExecutionOptimization::IndexRangeLimitPushdown,
    )?))
}
