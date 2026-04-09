//! Module: executor::kernel
//! Responsibility: unified read-execution kernel orchestration boundaries.
//! Does not own: logical planning or physical access path lowering policies.
//! Boundary: key-stream decoration, materialization, and residual retry behavior.

use crate::{
    db::{
        executor::{
            ExecutionOptimization, ExecutionPlan, ScalarContinuationBindings,
            pipeline::contracts::{
                DirectCoveringScanMaterializationRequest, ExecutionInputs,
                MaterializedExecutionAttempt, ResolvedExecutionKeyStream,
                RowCollectorMaterializationRequest, RuntimePageMaterializationRequest,
                StructuralCursorPage,
            },
            pipeline::operators::decorate_resolved_execution_key_stream,
            route::{IndexRangeLimitSpec, widened_residual_predicate_pushdown_fetch},
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
        let initial_retry_decision = Self::index_range_limited_residual_retry_decision(
            inputs.plan(),
            continuation,
            route_plan,
            probe_attempt.rows_scanned,
            probe_attempt.post_access_rows,
        );
        let mut accumulated_attempt = probe_attempt;
        let Some(mut retry_fetch) = initial_retry_decision.widened_fetch() else {
            if initial_retry_decision.requires_unbounded_fallback() {
                let fallback_attempt = Self::materialize_route_attempt(
                    inputs,
                    &Self::unbounded_residual_retry_route_plan(route_plan),
                    continuation,
                    predicate_compile_mode,
                )?;

                return Ok(Self::merge_retry_attempts(
                    accumulated_attempt,
                    fallback_attempt,
                ));
            }

            return Ok(accumulated_attempt);
        };

        // Phase 2: widen the bounded pushdown fetch while it remains under the
        // residual safety cap so selective predicates can satisfy the window
        // without immediately degrading into a full unbounded retry.
        let mut retry_route_plan = route_plan.clone();
        loop {
            Self::apply_index_range_retry_fetch(&mut retry_route_plan, retry_fetch);
            let retry_attempt = Self::materialize_route_attempt(
                inputs,
                &retry_route_plan,
                continuation,
                predicate_compile_mode,
            )?;
            let retry_decision = Self::index_range_limited_residual_retry_decision(
                inputs.plan(),
                continuation,
                &retry_route_plan,
                retry_attempt.rows_scanned,
                retry_attempt.post_access_rows,
            );
            accumulated_attempt = Self::merge_retry_attempts(accumulated_attempt, retry_attempt);

            if let Some(next_retry_fetch) = retry_decision.widened_fetch() {
                retry_fetch = next_retry_fetch;
                continue;
            }
            if retry_decision.requires_unbounded_fallback() {
                let fallback_attempt = Self::materialize_route_attempt(
                    inputs,
                    &Self::unbounded_residual_retry_route_plan(route_plan),
                    continuation,
                    predicate_compile_mode,
                )?;

                return Ok(Self::merge_retry_attempts(
                    accumulated_attempt,
                    fallback_attempt,
                ));
            }

            return Ok(accumulated_attempt);
        }
    }

    // Materialize one structural attempt for a specific route-plan candidate.
    fn materialize_route_attempt(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<MaterializedExecutionAttempt, InternalError> {
        // Phase 0: let the cursorless SQL covering-scan short path win before
        // the executor pays to resolve a generic ordered key stream that the
        // terminal would immediately ignore and rescan.
        if let Some(direct_covering_attempt) =
            Self::try_materialize_direct_covering_route_attempt(inputs, route_plan, continuation)?
        {
            return Ok(direct_covering_attempt);
        }

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

    // Materialize one cursorless SQL covering-scan attempt before generic
    // key-stream resolution when the same route-owned covering contract can
    // already produce the final structural page directly.
    fn try_materialize_direct_covering_route_attempt(
        inputs: &ExecutionInputs<'_>,
        route_plan: &ExecutionPlan,
        continuation: ScalarContinuationBindings<'_>,
    ) -> Result<Option<MaterializedExecutionAttempt>, InternalError> {
        let Some((page, keys_scanned, post_access_rows)) = inputs
            .runtime()
            .try_materialize_load_via_direct_covering_scan(
                DirectCoveringScanMaterializationRequest {
                    plan: inputs.plan(),
                    scan_budget_hint: route_plan.scan_hints.load_scan_budget_hint,
                    cursor_boundary: continuation.post_access_cursor_boundary(),
                    load_terminal_fast_path: route_plan.load_terminal_fast_path(),
                    predicate_slots: inputs.execution_preparation().compiled_predicate(),
                    validate_projection: inputs.validate_projection(),
                    retain_slot_rows: inputs.retain_slot_rows(),
                    prefer_rendered_projection_rows: inputs.prefer_rendered_projection_rows(),
                },
            )?
        else {
            return Ok(None);
        };

        Ok(Some(MaterializedExecutionAttempt {
            page,
            rows_scanned: keys_scanned,
            post_access_rows,
            optimization: Self::direct_covering_route_optimization(route_plan),
            index_predicate_applied: false,
            index_predicate_keys_rejected: 0,
            distinct_keys_deduped: 0,
        }))
    }

    // Project the route-owned fast-path optimization label onto direct
    // covering-scan attempts that bypass generic key-stream construction.
    const fn direct_covering_route_optimization(
        route_plan: &ExecutionPlan,
    ) -> Option<ExecutionOptimization> {
        if route_plan.index_range_limit_fast_path_enabled() {
            return Some(ExecutionOptimization::IndexRangeLimitPushdown);
        }

        if route_plan.secondary_fast_path_eligible() {
            return Some(if route_plan.top_n_seek_spec().is_some() {
                ExecutionOptimization::SecondaryOrderTopNSeek
            } else {
                ExecutionOptimization::SecondaryOrderPushdown
            });
        }

        if route_plan.pk_order_fast_path_eligible() {
            return Some(if route_plan.top_n_seek_spec().is_some() {
                ExecutionOptimization::PrimaryKeyTopNSeek
            } else {
                ExecutionOptimization::PrimaryKey
            });
        }

        None
    }

    // Merge retry attempts under canonical residual-retry accounting.
    fn merge_retry_attempts(
        mut accumulated_attempt: MaterializedExecutionAttempt,
        latest_attempt: MaterializedExecutionAttempt,
    ) -> MaterializedExecutionAttempt {
        accumulated_attempt.rows_scanned = accumulated_attempt
            .rows_scanned
            .saturating_add(latest_attempt.rows_scanned);
        accumulated_attempt.optimization = latest_attempt.optimization;
        accumulated_attempt.index_predicate_applied =
            accumulated_attempt.index_predicate_applied || latest_attempt.index_predicate_applied;
        accumulated_attempt.index_predicate_keys_rejected = accumulated_attempt
            .index_predicate_keys_rejected
            .saturating_add(latest_attempt.index_predicate_keys_rejected);
        accumulated_attempt.distinct_keys_deduped = accumulated_attempt
            .distinct_keys_deduped
            .saturating_add(latest_attempt.distinct_keys_deduped);
        accumulated_attempt.page = latest_attempt.page;
        accumulated_attempt.post_access_rows = latest_attempt.post_access_rows;

        accumulated_attempt
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
                load_order_route_contract: route_plan.load_order_route_contract(),
                continuation,
                cursor_boundary: continuation.post_access_cursor_boundary(),
                load_terminal_fast_path: route_plan.load_terminal_fast_path(),
                predicate_slots: inputs.execution_preparation().compiled_predicate(),
                validate_projection: inputs.validate_projection(),
                retain_slot_rows: inputs.retain_slot_rows(),
                slot_only_required_slots: inputs.slot_only_required_slots(),
                prefer_rendered_projection_rows: inputs.prefer_rendered_projection_rows(),
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
                load_order_route_contract: route_plan.load_order_route_contract(),
                validate_projection: inputs.validate_projection(),
                retain_slot_rows: inputs.retain_slot_rows(),
                slot_only_required_slots: inputs.slot_only_required_slots(),
                cursor_emission: if inputs.emit_cursor() {
                    crate::db::executor::pipeline::contracts::CursorEmissionMode::Emit
                } else {
                    crate::db::executor::pipeline::contracts::CursorEmissionMode::Suppress
                },
                consistency: inputs.consistency(),
                continuation,
            })?;

        Ok((page, keys_scanned, post_access_rows))
    }

    // Decide whether residual underfill should stop, widen the bounded fetch,
    // or fall back to an unbounded retry.
    fn index_range_limited_residual_retry_decision(
        plan: &AccessPlannedQuery,
        continuation: ScalarContinuationBindings<'_>,
        route_plan: &ExecutionPlan,
        rows_scanned: usize,
        post_access_rows: usize,
    ) -> ResidualRetryDecision {
        let logical = plan.scalar_plan();
        let Some(limit_spec) = route_plan.index_range_limit_spec else {
            return ResidualRetryDecision::None;
        };
        if logical.predicate.is_none() {
            return ResidualRetryDecision::None;
        }
        if limit_spec.fetch == 0 {
            return ResidualRetryDecision::None;
        }
        let Some(limit) = logical.page.as_ref().and_then(|page| page.limit) else {
            return ResidualRetryDecision::None;
        };
        let keep_count = continuation.keep_count_for_limit_window(plan, limit);
        if keep_count == 0 {
            return ResidualRetryDecision::None;
        }
        if rows_scanned < limit_spec.fetch {
            return ResidualRetryDecision::None;
        }
        if post_access_rows >= keep_count {
            return ResidualRetryDecision::None;
        }

        widened_residual_predicate_pushdown_fetch(limit_spec.fetch, keep_count, post_access_rows)
            .map_or(ResidualRetryDecision::FallbackUnbounded, |fetch| {
                ResidualRetryDecision::WidenBoundedFetch { fetch }
            })
    }

    // Apply one widened residual-retry fetch to the bounded index-range route
    // contract and the coupled scan-budget hints that consume the same window.
    const fn apply_index_range_retry_fetch(route_plan: &mut ExecutionPlan, fetch: usize) {
        if route_plan.index_range_limit_spec.is_some() {
            route_plan.index_range_limit_spec = Some(IndexRangeLimitSpec { fetch });
        }
        if route_plan.scan_hints.load_scan_budget_hint.is_some() {
            route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
        }
        if route_plan.scan_hints.physical_fetch_hint.is_some() {
            route_plan.scan_hints.physical_fetch_hint = Some(fetch);
        }
    }

    // Build the terminal residual-retry fallback plan by clearing the bounded
    // pushdown spec and coupled scan-budget hints before re-materialization.
    fn unbounded_residual_retry_route_plan(route_plan: &ExecutionPlan) -> ExecutionPlan {
        let mut fallback_route_plan = route_plan.clone();
        fallback_route_plan.index_range_limit_spec = None;
        fallback_route_plan.scan_hints.load_scan_budget_hint = None;
        fallback_route_plan.scan_hints.physical_fetch_hint = None;

        fallback_route_plan
    }
}

#[derive(Clone, Copy)]
enum ResidualRetryDecision {
    None,
    WidenBoundedFetch { fetch: usize },
    FallbackUnbounded,
}

impl ResidualRetryDecision {
    const fn widened_fetch(self) -> Option<usize> {
        match self {
            Self::WidenBoundedFetch { fetch } => Some(fetch),
            Self::None | Self::FallbackUnbounded => None,
        }
    }

    const fn requires_unbounded_fallback(self) -> bool {
        matches!(self, Self::FallbackUnbounded)
    }
}
