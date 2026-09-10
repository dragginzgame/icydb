//! Module: executor::pipeline::runtime::fast_path
//! Responsibility: fast-path decision and fallback key-stream resolution policy.
//! Does not own: page materialization.
//! Boundary: internal helper boundary for `executor::pipeline::runtime`.

mod strategy;

use strategy::evaluate_fast_path;

use crate::{
    db::{
        executor::{
            AccessStreamExecutionPolicy, ExecutionRoutePlan,
            pipeline::{contracts::ResolvedExecutionKeyStream, runtime::ExecutionAttemptKernel},
        },
        index::{IndexCompilePolicy, predicate::IndexPredicateExecution},
    },
    error::InternalError,
};

impl ExecutionAttemptKernel<'_> {
    // Resolve the canonical access stream when no fast path produced rows.
    fn resolve_fallback_execution_key_stream(
        &self,
        route_plan: &ExecutionRoutePlan,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let fallback_fetch_hint =
            route_plan.fallback_physical_fetch_hint(self.inputs.stream_bindings().direction());
        let execution_policy = AccessStreamExecutionPolicy::new(
            fallback_fetch_hint,
            route_plan.index_leaf_order_policy(),
        );
        let key_stream = self
            .inputs
            .runtime()
            .resolve_fallback_execution_key_stream(
                self.inputs.executable_access().clone(),
                *self.inputs.stream_bindings(),
                execution_policy,
                index_predicate_execution,
            )?;

        Ok(ResolvedExecutionKeyStream::new(key_stream, None))
    }

    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct(
        &self,
        route_plan: &ExecutionRoutePlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        // Phase 0: reuse precompiled runtime index predicates when the
        // execution-preparation boundary already owns the requested mode, and
        // only fall back to one on-demand compile when it does not.
        let index_predicate_program = self
            .inputs
            .execution_preparation()
            .resolve_index_program(predicate_compile_mode);
        let index_predicate_execution = index_predicate_program.as_deref();

        // Phase 1: streaming routes try canonical fast-path precedence before
        // falling back; materialized routes proceed directly to fallback.
        let fast_path_decision = if route_plan.is_streaming() {
            evaluate_fast_path(self.inputs, route_plan, index_predicate_execution)?
        } else {
            None
        };

        // Phase 2: materialize from a fast-path hit or canonical fallback stream.
        match fast_path_decision {
            Some(fast) => Ok(ResolvedExecutionKeyStream::new(
                fast.ordered_key_stream,
                fast.rows_scanned,
            )),
            None => {
                self.resolve_fallback_execution_key_stream(route_plan, index_predicate_execution)
            }
        }
    }
}
