//! Module: executor::load::fast_stream
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
        Context,
        executor::{
            AccessExecutionDescriptor, ExecutionOptimization,
            load::{FastPathKeyResult, LoadExecutor, invariant},
            route::RoutedKeyStreamRequest,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one fast-path access stream without materialize/restream adapters.
    ///
    /// Fast-path streams must expose an exact key-count hint for observability parity.
    pub(super) fn execute_fast_stream_request(
        ctx: &Context<'_, E>,
        descriptor: AccessExecutionDescriptor<'_, E::Key>,
        optimization: ExecutionOptimization,
    ) -> Result<FastPathKeyResult, InternalError> {
        // Phase 1: resolve the ordered key stream through the routed access boundary.
        let key_stream = Self::resolve_routed_key_stream(
            ctx,
            RoutedKeyStreamRequest::AccessDescriptor(descriptor),
        )?;

        // Phase 2: enforce exact row-scan count hint required by fast-path observability.
        let rows_scanned = key_stream
            .exact_key_count_hint()
            .ok_or_else(|| invariant("fast-path stream must expose an exact key-count hint"))?;

        Ok(FastPathKeyResult {
            ordered_key_stream: key_stream,
            rows_scanned,
            optimization,
        })
    }
}
