//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

use crate::{
    db::{
        executor::{
            ExecutionPlan, ScalarContinuationBindings,
            pipeline::contracts::{
                CursorPage, ExecutionInputsProjection, LoadExecutor, MaterializedExecutionAttempt,
                ResolvedExecutionKeyStream,
            },
            pipeline::operators::decorate_resolved_execution_key_stream,
            terminal::page::PageMaterializationRequest,
        },
        index::IndexCompilePolicy,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionKernel
///
/// Canonical kernel boundary for read execution unification.
///

pub(in crate::db::executor) struct ExecutionKernel;

impl ExecutionKernel {
    /// Resolve one execution key stream under kernel-owned DISTINCT decoration.
    pub(in crate::db::executor) fn resolve_execution_key_stream<E, I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        let resolved = LoadExecutor::<E>::resolve_execution_key_stream_without_distinct(
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
    pub(in crate::db::executor) fn materialize_with_optional_residual_retry<E, I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
        // Phase 1: inject typed route-attempt and retry-gate callbacks into
        // the shared retry orchestration boundary.
        let mut materialize_route_attempt = |candidate_route_plan: &ExecutionPlan| {
            Self::materialize_route_attempt::<E, I>(
                inputs,
                candidate_route_plan,
                continuation,
                predicate_compile_mode,
            )
        };
        let mut retry_required_for_probe = |probe_attempt: &MaterializedExecutionAttempt<E>| {
            Self::index_range_limited_residual_retry_required(
                inputs.plan(),
                continuation,
                route_plan,
                probe_attempt.rows_scanned,
                probe_attempt.post_access_rows,
            )
        };
        Self::materialize_with_optional_residual_retry_control_flow(
            route_plan,
            &mut materialize_route_attempt,
            &mut retry_required_for_probe,
        )
    }

    // Materialize one typed attempt for a specific route-plan candidate.
    fn materialize_route_attempt<E, I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError>
    where
        E: EntityKind + EntityValue,
        I: ExecutionInputsProjection<E>,
    {
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
    fn merge_probe_with_fallback<E: EntityKind>(
        mut probe_attempt: MaterializedExecutionAttempt<E>,
        fallback_attempt: MaterializedExecutionAttempt<E>,
    ) -> MaterializedExecutionAttempt<E> {
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

    // Shared residual-retry orchestrator for scalar load materialization
    // attempts over typed payloads.
    fn materialize_with_optional_residual_retry_typed<E>(
        route_plan: &ExecutionPlan,
        probe_attempt: MaterializedExecutionAttempt<E>,
        retry_required: bool,
        materialize_route_attempt: &mut dyn FnMut(
            &ExecutionPlan,
        ) -> Result<
            MaterializedExecutionAttempt<E>,
            InternalError,
        >,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError>
    where
        E: EntityKind,
    {
        if !retry_required {
            return Ok(probe_attempt);
        }

        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        let fallback_attempt = materialize_route_attempt(&fallback_route_plan)?;

        Ok(Self::merge_probe_with_fallback(
            probe_attempt,
            fallback_attempt,
        ))
    }

    // Shared retry control flow for routed materialization attempts.
    // Probe/fallback materialization and retry gating are callback-injected so
    // this orchestration body stays isolated from route-resolution details.
    fn materialize_with_optional_residual_retry_control_flow<E>(
        route_plan: &ExecutionPlan,
        materialize_route_attempt: &mut dyn FnMut(
            &ExecutionPlan,
        ) -> Result<
            MaterializedExecutionAttempt<E>,
            InternalError,
        >,
        retry_required_for_probe: &mut dyn FnMut(&MaterializedExecutionAttempt<E>) -> bool,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError>
    where
        E: EntityKind,
    {
        // Phase 1: materialize one probe attempt for the routed plan.
        let probe_attempt = materialize_route_attempt(route_plan)?;
        let retry_required = retry_required_for_probe(&probe_attempt);

        // Phase 2: apply canonical fallback control flow when retry is required.
        Self::materialize_with_optional_residual_retry_typed(
            route_plan,
            probe_attempt,
            retry_required,
            materialize_route_attempt,
        )
    }

    // Materialize one already-resolved key stream using row-collector fast path
    // when applicable, otherwise fall back to canonical load materialization.
    fn materialize_resolved_execution_stream<E>(
        inputs: &impl ExecutionInputsProjection<E>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        resolved: &mut ResolvedExecutionKeyStream,
    ) -> Result<(CursorPage<E>, usize, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if let Some((page, keys_scanned, post_access_rows)) =
            Self::try_materialize_load_via_row_collector(
                inputs.ctx(),
                inputs.plan(),
                continuation.post_access_cursor_boundary(),
                resolved.key_stream_mut(),
            )?
        {
            return Ok((page, keys_scanned, post_access_rows));
        }

        LoadExecutor::<E>::materialize_key_stream_into_page(PageMaterializationRequest {
            ctx: inputs.ctx(),
            plan: inputs.plan(),
            predicate_slots: inputs.execution_preparation().compiled_predicate(),
            key_stream: resolved.key_stream_mut(),
            scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
            stream_order_contract_safe: route_plan.stream_order_contract_safe(),
            consistency: inputs.consistency(),
            continuation,
        })
    }

    // Retry index-range limit pushdown when a bounded residual-filter pass may
    // have under-filled the requested page window.
    fn index_range_limited_residual_retry_required<K>(
        plan: &AccessPlannedQuery<K>,
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
