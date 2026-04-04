//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

use crate::{
    db::{
        executor::{
            ExecutionPlan, ScalarContinuationBindings,
            pipeline::contracts::{
                ExecutionInputs, MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
                RowCollectorMaterializationRequest, RuntimePageMaterializationRequest,
                StructuralCursorPage,
            },
            pipeline::operators::decorate_resolved_execution_key_stream,
        },
        index::IndexCompilePolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

///
/// ExecutionKernel
///
/// Canonical kernel boundary for read execution unification.
///

pub(in crate::db::executor) struct ExecutionKernel;

impl ExecutionKernel {
    /// Resolve one execution key stream under kernel-owned DISTINCT decoration.
    pub(in crate::db::executor) fn resolve_execution_key_stream(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let resolved = Self::resolve_execution_key_stream_without_distinct(
            inputs,
            route_plan,
            predicate_compile_mode,
        )?;

        Ok(decorate_resolved_execution_key_stream(
            resolved,
            inputs.plan(),
            inputs.stream_bindings().direction(),
        ))
    }

    /// Materialize one load execution attempt with optional residual retry.
    pub(in crate::db::executor) fn materialize_with_optional_residual_retry(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        // Phase 1: materialize one probe attempt for the planned route.
        let probe_attempt = Self::materialize_route_attempt(
            inputs,
            route_plan,
            continuation,
            predicate_compile_mode,
        )?;
        let retry_required = Self::index_range_limited_residual_retry_required(
            inputs.plan(),
            continuation,
            route_plan,
            probe_attempt.rows_scanned,
            probe_attempt.post_access_rows,
        );
        if !retry_required {
            return Ok(probe_attempt);
        }

        // Phase 2: retry once without index-range limit pushdown when the
        // probe under-fills the requested post-access keep window.
        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        let fallback_attempt = Self::materialize_route_attempt(
            inputs,
            &fallback_route_plan,
            continuation,
            predicate_compile_mode,
        )?;

        Ok(Self::merge_probe_with_fallback(
            probe_attempt,
            fallback_attempt,
        ))
    }

    // Materialize one structural attempt for a specific route-plan candidate.
    fn materialize_route_attempt(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        let mut resolved =
            Self::resolve_execution_key_stream(inputs, route_plan, predicate_compile_mode)?;
        let (page, keys_scanned, post_access_rows) = Self::materialize_resolved_execution_stream(
            inputs,
            route_plan,
            continuation,
            &mut resolved,
        )?;
        let rows_scanned = resolved.rows_scanned_override().unwrap_or(keys_scanned);

        Ok(MaterializedExecutionAttempt {
            page,
            rows_scanned,
            post_access_rows,
            optimization: resolved.optimization(),
            index_predicate_applied: resolved.index_predicate_applied(),
            index_predicate_keys_rejected: resolved.index_predicate_keys_rejected(),
            distinct_keys_deduped: resolved.distinct_keys_deduped(),
        })
    }

    // Merge probe and fallback attempts under canonical residual-retry accounting.
    fn merge_probe_with_fallback(
        mut probe_attempt: MaterializedExecutionAttempt,
        fallback_attempt: MaterializedExecutionAttempt,
    ) -> MaterializedExecutionAttempt {
        probe_attempt.rows_scanned = probe_attempt
            .rows_scanned
            .saturating_add(fallback_attempt.rows_scanned);
        probe_attempt.optimization = fallback_attempt.optimization;
        probe_attempt.index_predicate_applied =
            probe_attempt.index_predicate_applied || fallback_attempt.index_predicate_applied;
        probe_attempt.index_predicate_keys_rejected = probe_attempt
            .index_predicate_keys_rejected
            .saturating_add(fallback_attempt.index_predicate_keys_rejected);
        probe_attempt.distinct_keys_deduped = probe_attempt
            .distinct_keys_deduped
            .saturating_add(fallback_attempt.distinct_keys_deduped);
        probe_attempt.page = fallback_attempt.page;
        probe_attempt.post_access_rows = fallback_attempt.post_access_rows;

        probe_attempt
    }

    // Materialize one already-resolved key stream using row-collector fast path
    // when applicable, otherwise fall back to canonical load materialization.
    fn materialize_resolved_execution_stream(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        resolved: &mut ResolvedExecutionKeyStream,
    ) -> Result<(StructuralCursorPage, usize, usize), InternalError> {
        if let Some((page, keys_scanned, post_access_rows)) = inputs
            .runtime()
            .try_materialize_load_via_row_collector(RowCollectorMaterializationRequest {
                plan: inputs.plan(),
                scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
                stream_order_contract_safe: route_plan.stream_order_contract_safe(),
                continuation,
                cursor_boundary: continuation.post_access_cursor_boundary(),
                predicate_slots: inputs.execution_preparation().compiled_predicate(),
                validate_projection: inputs.validate_projection(),
                retain_slot_rows: inputs.retain_slot_rows(),
                key_stream: resolved.key_stream_mut(),
            })?
        {
            return Ok((page, keys_scanned, post_access_rows));
        }

        let (page, keys_scanned, post_access_rows) = inputs
            .runtime()
            .materialize_key_stream_into_structural_page(RuntimePageMaterializationRequest {
                plan: inputs.plan(),
                predicate_slots: inputs.execution_preparation().compiled_predicate(),
                key_stream: resolved.key_stream_mut(),
                scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
                stream_order_contract_safe: route_plan.stream_order_contract_safe(),
                validate_projection: inputs.validate_projection(),
                retain_slot_rows: inputs.retain_slot_rows(),
                consistency: inputs.consistency(),
                continuation,
            })?;

        Ok((page, keys_scanned, post_access_rows))
    }

    // Retry index-range limit pushdown when a bounded residual-filter pass may
    // have under-filled the requested page window.
    fn index_range_limited_residual_retry_required(
        plan: &AccessPlannedQuery,
        continuation: ScalarContinuationBindings<'_>,
        route_plan: &ExecutionPlan,
        rows_scanned: usize,
        post_access_rows: usize,
    ) -> bool {
        let logical = plan.scalar_plan();
        let Some(limit_spec) = route_plan.index_range_limit_spec else {
            return false;
        };
        if logical.predicate.is_none() {
            return false;
        }
        if limit_spec.fetch == 0 {
            return false;
        }
        let Some(limit) = logical.page.as_ref().and_then(|page| page.limit) else {
            return false;
        };
        let keep_count = continuation.keep_count_for_limit_window(plan, limit);
        if keep_count == 0 {
            return false;
        }
        if rows_scanned < limit_spec.fetch {
            return false;
        }

        post_access_rows < keep_count
    }
}
