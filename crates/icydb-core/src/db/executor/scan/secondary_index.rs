//! Module: executor::scan::secondary_index
//! Responsibility: secondary-index ordered fast-path stream execution helpers.
//! Does not own: planner eligibility derivation or cursor continuation semantics.
//! Boundary: consumes lowered index-prefix specs and emits ordered key streams.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        direction::Direction,
        executor::{
            AccessStreamBindings, AccessStreamExecutionPolicy, ExecutionOptimization,
            LoweredIndexPrefixSpec, pipeline::contracts::FastPathKeyResult,
            scan::fast_stream::execute_structural_fast_stream_request,
            stream::access::TraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};

/// Execute one secondary-index fast-path stream route through the structural runtime.
pub(in crate::db::executor) fn execute_secondary_index_fast_stream_route(
    runtime: &TraversalRuntime,
    _plan: &AccessPlannedQuery,
    executable: &ExecutableAccessPlan<'_, Value>,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    stream_direction: Direction,
    probe_fetch_hint: Option<usize>,
    index_predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    // Phase 1: verify structural access-path/spec invariants for index-prefix execution.
    let Some(executable_path) = executable.as_path() else {
        return Ok(None);
    };
    let path_facts = executable_path.shape_facts();
    let Some(details) = path_facts.index_prefix_details() else {
        return Ok(None);
    };
    if index_prefix_specs.len() != path_facts.index_prefix_spec_count() {
        return Err(InternalError::secondary_index_prefix_spec_required());
    }
    if index_prefix_specs.is_empty() {
        return Ok(None);
    }
    debug_assert!(
        index_prefix_specs
            .iter()
            .all(|spec| spec.scan_contract().name() == details.name()),
        "secondary fast-path spec/index alignment must be validated by resolver",
    );

    // Phase 2: bind execution inputs and run the shared fast-stream boundary.
    let fast = execute_structural_fast_stream_request(
        runtime,
        executable,
        AccessStreamBindings::with_index_prefixes(index_prefix_specs, stream_direction),
        AccessStreamExecutionPolicy::new(
            probe_fetch_hint,
            crate::db::executor::IndexLeafOrderPolicy::PreservePrefixBranch,
        ),
        index_predicate_execution,
        ExecutionOptimization::SecondaryOrderPushdown,
    )?;
    if let Some(fetch) = probe_fetch_hint {
        debug_assert!(
            fast.rows_scanned
                .is_none_or(|rows_scanned| rows_scanned <= fetch),
            "secondary fast-path rows_scanned must not exceed bounded fetch",
        );
    }

    Ok(Some(fast))
}
