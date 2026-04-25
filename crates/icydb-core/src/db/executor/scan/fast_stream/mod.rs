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
    db::executor::{
        ExecutableAccess, ExecutionOptimization, OrderedKeyStreamBox,
        pipeline::contracts::FastPathKeyResult, stream::access::TraversalRuntime,
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
    access: ExecutableAccess<'_, Value>,
    optimization: ExecutionOptimization,
) -> Result<FastPathKeyResult, InternalError> {
    let key_stream = runtime.ordered_key_stream_from_runtime_access(access)?;

    Ok(finalize_fast_path_key_stream(key_stream, optimization))
}
