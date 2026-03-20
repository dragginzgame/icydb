//! Module: executor::scan::fast_stream
//! Responsibility: execute verified fast-path stream requests without restream adapters.
//! Does not own: fast-path eligibility policy or access-path lowering rules.
//! Boundary: stream execution helper used after routing/eligibility gates.

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::{
    db::{Context, executor::pipeline::contracts::LoadExecutor},
    traits::{EntityKind, EntityValue},
};
use crate::{
    db::{
        access::StructuralKey,
        executor::{
            ExecutableAccess, ExecutionOptimization, OrderedKeyStreamBox,
            pipeline::contracts::FastPathKeyResult, stream::access::StructuralTraversalRuntime,
        },
    },
    error::InternalError,
};

// Enforce exact row-count observability required by fast-path stream execution.
fn finalize_fast_path_key_stream(
    key_stream: OrderedKeyStreamBox,
    optimization: ExecutionOptimization,
) -> Result<FastPathKeyResult, InternalError> {
    let rows_scanned = key_stream.exact_key_count_hint().ok_or_else(|| {
        crate::db::error::query_executor_invariant(
            "fast-path stream must expose an exact key-count hint",
        )
    })?;

    Ok(FastPathKeyResult {
        ordered_key_stream: key_stream,
        rows_scanned,
        optimization,
    })
}

/// Resolve one structural fast-path access stream without rebuilding a typed access plan.
pub(in crate::db::executor) fn execute_structural_fast_stream_request(
    runtime: &StructuralTraversalRuntime,
    access: ExecutableAccess<'_, StructuralKey>,
    optimization: ExecutionOptimization,
) -> Result<FastPathKeyResult, InternalError> {
    let key_stream = runtime.ordered_key_stream_from_structural_runtime_access(access)?;

    finalize_fast_path_key_stream(key_stream, optimization)
}

#[cfg(test)]
impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one fast-path access stream without materialize/restream adapters.
    ///
    /// Fast-path streams must expose an exact key-count hint for observability parity.
    pub(super) fn execute_fast_stream_request(
        ctx: &Context<'_, E>,
        access: ExecutableAccess<'_, E::Key>,
        optimization: ExecutionOptimization,
    ) -> Result<FastPathKeyResult, InternalError> {
        let key_stream = ctx.ordered_key_stream_from_runtime_access(access)?;

        finalize_fast_path_key_stream(key_stream, optimization)
    }
}
