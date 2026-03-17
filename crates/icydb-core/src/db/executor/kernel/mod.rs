//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

use crate::{
    db::{
        executor::{
            ExecutionOptimization, ExecutionPlan, ScalarContinuationBindings,
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
use std::any::Any;

///
/// ExecutionKernel
///
/// Canonical kernel boundary for read execution unification.
///

pub(in crate::db::executor) struct ExecutionKernel;

///
/// DynMaterializedExecutionAttempt
///
/// Erased scalar materialization payload used by shared retry control flow.
/// Typed wrappers convert to/from `CursorPage<E>` so retry orchestration can
/// stay non-generic while decode/materialization remain typed.
///

struct DynMaterializedExecutionAttempt {
    page: Box<dyn Any>,
    rows_scanned: usize,
    post_access_rows: usize,
    optimization: Option<ExecutionOptimization>,
    index_predicate_applied: bool,
    index_predicate_keys_rejected: u64,
    distinct_keys_deduped: u64,
}

impl DynMaterializedExecutionAttempt {
    // Build one erased attempt from typed materialization payloads.
    fn from_typed<E: EntityKind + EntityValue>(
        page: CursorPage<E>,
        rows_scanned: usize,
        post_access_rows: usize,
        optimization: Option<ExecutionOptimization>,
        index_predicate_applied: bool,
        index_predicate_keys_rejected: u64,
        distinct_keys_deduped: u64,
    ) -> Self {
        Self {
            page: Box::new(page),
            rows_scanned,
            post_access_rows,
            optimization,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        }
    }

    // Convert one erased attempt back into typed load materialization payloads.
    fn into_typed<E: EntityKind + EntityValue>(
        self,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError> {
        let page = self.page.downcast::<CursorPage<E>>().map_err(|_| {
            crate::db::error::query_executor_invariant(
                "dynamic scalar materialization page downcast failed",
            )
        })?;

        Ok(MaterializedExecutionAttempt {
            page: *page,
            rows_scanned: self.rows_scanned,
            post_access_rows: self.post_access_rows,
            optimization: self.optimization,
            index_predicate_applied: self.index_predicate_applied,
            index_predicate_keys_rejected: self.index_predicate_keys_rejected,
            distinct_keys_deduped: self.distinct_keys_deduped,
        })
    }

    // Merge probe and fallback attempts under canonical residual-retry accounting.
    fn merge_probe_with_fallback(mut self, fallback: Self) -> Self {
        self.rows_scanned = self.rows_scanned.saturating_add(fallback.rows_scanned);
        self.optimization = fallback.optimization;
        self.index_predicate_applied =
            self.index_predicate_applied || fallback.index_predicate_applied;
        self.index_predicate_keys_rejected = self
            .index_predicate_keys_rejected
            .saturating_add(fallback.index_predicate_keys_rejected);
        self.distinct_keys_deduped = self
            .distinct_keys_deduped
            .saturating_add(fallback.distinct_keys_deduped);
        self.page = fallback.page;
        self.post_access_rows = fallback.post_access_rows;

        self
    }
}

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
        // the shared dynamic retry orchestration boundary.
        let mut materialize_route_attempt = |candidate_route_plan: &ExecutionPlan| {
            Self::materialize_route_attempt_dyn::<E, I>(
                inputs,
                candidate_route_plan,
                continuation,
                predicate_compile_mode,
            )
        };
        let mut retry_required_for_probe = |probe_attempt: &DynMaterializedExecutionAttempt| {
            Self::index_range_limited_residual_retry_required(
                inputs.plan(),
                continuation,
                route_plan,
                probe_attempt.rows_scanned,
                probe_attempt.post_access_rows,
            )
        };
        let attempt = Self::materialize_with_optional_residual_retry_control_flow(
            route_plan,
            &mut materialize_route_attempt,
            &mut retry_required_for_probe,
        )?;

        // Phase 2: project the erased attempt back to typed response payloads.
        attempt.into_typed::<E>()
    }

    // Materialize one typed attempt for a specific route-plan candidate and
    // return an erased payload for shared retry orchestration.
    fn materialize_route_attempt_dyn<E, I>(
        inputs: &I,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<DynMaterializedExecutionAttempt, InternalError>
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

        Ok(DynMaterializedExecutionAttempt::from_typed(
            page,
            rows_scanned,
            post_access_rows,
            resolved.optimization(),
            resolved.index_predicate_applied(),
            resolved.index_predicate_keys_rejected(),
            resolved.distinct_keys_deduped(),
        ))
    }

    // Shared non-generic residual-retry orchestrator for scalar load
    // materialization attempts.
    fn materialize_with_optional_residual_retry_dyn(
        route_plan: &ExecutionPlan,
        probe_attempt: DynMaterializedExecutionAttempt,
        retry_required: bool,
        materialize_route_attempt: &mut dyn FnMut(
            &ExecutionPlan,
        ) -> Result<
            DynMaterializedExecutionAttempt,
            InternalError,
        >,
    ) -> Result<DynMaterializedExecutionAttempt, InternalError> {
        if !retry_required {
            return Ok(probe_attempt);
        }

        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        let fallback_attempt = materialize_route_attempt(&fallback_route_plan)?;

        Ok(probe_attempt.merge_probe_with_fallback(fallback_attempt))
    }

    // Shared retry control flow for routed materialization attempts.
    // Probe/fallback materialization and retry gating are callback-injected so
    // this orchestration body can stay non-generic.
    fn materialize_with_optional_residual_retry_control_flow(
        route_plan: &ExecutionPlan,
        materialize_route_attempt: &mut dyn FnMut(
            &ExecutionPlan,
        ) -> Result<
            DynMaterializedExecutionAttempt,
            InternalError,
        >,
        retry_required_for_probe: &mut dyn FnMut(&DynMaterializedExecutionAttempt) -> bool,
    ) -> Result<DynMaterializedExecutionAttempt, InternalError> {
        // Phase 1: materialize one probe attempt for the routed plan.
        let probe_attempt = materialize_route_attempt(route_plan)?;
        let retry_required = retry_required_for_probe(&probe_attempt);

        // Phase 2: apply canonical fallback control flow when retry is required.
        Self::materialize_with_optional_residual_retry_dyn(
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
