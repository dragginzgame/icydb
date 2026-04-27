//! Module: executor::scan::fast_stream
//! Responsibility: execute verified fast-path stream requests without restream adapters.
//! Does not own: fast-path eligibility policy or access-path lowering rules.
//! Boundary: stream execution helper used after routing/eligibility gates.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::ExecutableAccessPlan,
        executor::{
            AccessStreamBindings, ExecutionOptimization, OrderedKeyStreamBox,
            pipeline::contracts::FastPathKeyResult, stream::access::TraversalRuntime,
        },
    },
    error::InternalError,
    value::Value,
};

// Enforce exact row-count observability required by fast-path stream execution.
fn finalize_fast_path_key_stream(
    key_stream: OrderedKeyStreamBox,
    optimization: ExecutionOptimization,
) -> FastPathKeyResult {
    let rows_scanned = key_stream.cheap_access_candidate_count_hint();

    FastPathKeyResult {
        ordered_key_stream: key_stream,
        rows_scanned,
        optimization,
    }
}

/// Resolve one structural fast-path access stream without rebuilding a typed access plan.
pub(in crate::db::executor) fn execute_structural_fast_stream_request(
    runtime: &TraversalRuntime,
    executable_access: &ExecutableAccessPlan<'_, Value>,
    bindings: AccessStreamBindings<'_>,
    physical_fetch_hint: Option<usize>,
    index_predicate_execution: Option<crate::db::index::predicate::IndexPredicateExecution<'_>>,
    optimization: ExecutionOptimization,
) -> Result<FastPathKeyResult, InternalError> {
    let key_stream = runtime.ordered_key_stream_from_executable_plan(
        executable_access,
        bindings,
        physical_fetch_hint,
        index_predicate_execution,
        false,
    )?;

    Ok(finalize_fast_path_key_stream(key_stream, optimization))
}
