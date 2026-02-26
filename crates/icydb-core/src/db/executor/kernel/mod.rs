mod aggregate;
mod distinct;
mod post_access;
mod predicate;
mod reducer;
mod window;

pub(in crate::db::executor) use post_access::{PlanRow, PostAccessStats};
pub(in crate::db::executor) use predicate::IndexPredicateCompileMode;

use crate::{
    db::{
        executor::{
            ExecutionPlan, OrderedKeyStreamBox,
            load::{
                ExecutionInputs, LoadExecutor, MaterializedExecutionAttempt,
                ResolvedExecutionKeyStream,
            },
        },
        query::{
            contracts::cursor::{ContinuationSignature, CursorBoundary},
            plan::{AccessPlannedQuery, Direction},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionKernel
///
/// Canonical kernel boundary for read execution unification.
/// Owns distinct decoration and residual retry orchestration in 0.30.
///

pub(in crate::db::executor) struct ExecutionKernel;

impl ExecutionKernel {
    // Resolve one execution key stream under kernel-owned DISTINCT decoration.
    pub(in crate::db::executor) fn resolve_execution_key_stream<E>(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        predicate_compile_mode: IndexPredicateCompileMode,
    ) -> Result<ResolvedExecutionKeyStream, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let resolved = LoadExecutor::<E>::resolve_execution_key_stream_without_distinct(
            inputs,
            route_plan,
            predicate_compile_mode,
        )?;

        Ok(distinct::decorate_resolved_execution_key_stream(
            resolved,
            inputs.plan,
            inputs.stream_bindings.direction,
        ))
    }

    // Apply canonical kernel DISTINCT decoration to one ordered key stream.
    pub(in crate::db::executor) fn decorate_key_stream_for_plan<K>(
        ordered_key_stream: OrderedKeyStreamBox,
        plan: &AccessPlannedQuery<K>,
        direction: Direction,
    ) -> OrderedKeyStreamBox {
        distinct::decorate_key_stream_for_plan(ordered_key_stream, plan, direction)
    }

    // Materialize one load execution attempt with optional residual retry
    // through the canonical kernel boundary.
    #[expect(clippy::too_many_lines)]
    pub(in crate::db::executor) fn materialize_with_optional_residual_retry<E>(
        inputs: &ExecutionInputs<'_, E>,
        route_plan: &ExecutionPlan,
        cursor_boundary: Option<&CursorBoundary>,
        continuation_signature: ContinuationSignature,
        predicate_compile_mode: IndexPredicateCompileMode,
    ) -> Result<MaterializedExecutionAttempt<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let mut resolved =
            Self::resolve_execution_key_stream(inputs, route_plan, predicate_compile_mode)?;
        let (mut page, keys_scanned, mut post_access_rows) =
            if let Some((page, keys_scanned, post_access_rows)) =
                Self::try_materialize_load_via_row_collector(
                    inputs.ctx,
                    inputs.plan,
                    cursor_boundary,
                    resolved.key_stream.as_mut(),
                )?
            {
                (page, keys_scanned, post_access_rows)
            } else {
                LoadExecutor::<E>::materialize_key_stream_into_page(
                    inputs.ctx,
                    inputs.plan,
                    inputs.predicate_slots,
                    resolved.key_stream.as_mut(),
                    route_plan.scan_hints.load_scan_budget_hint,
                    route_plan.streaming_access_shape_safe(),
                    cursor_boundary,
                    route_plan.direction(),
                    continuation_signature,
                )?
            };
        let mut rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        let mut optimization = resolved.optimization;
        let mut index_predicate_applied = resolved.index_predicate_applied;
        let mut index_predicate_keys_rejected = resolved.index_predicate_keys_rejected;
        let mut distinct_keys_deduped = resolved
            .distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get());

        if Self::index_range_limited_residual_retry_required(
            inputs.plan,
            cursor_boundary,
            route_plan,
            rows_scanned,
            post_access_rows,
        ) {
            let mut fallback_route_plan = route_plan.clone();
            fallback_route_plan.index_range_limit_spec = None;
            let mut fallback_resolved = Self::resolve_execution_key_stream(
                inputs,
                &fallback_route_plan,
                predicate_compile_mode,
            )?;
            let (fallback_page, fallback_keys_scanned, fallback_post_access_rows) =
                if let Some((fallback_page, fallback_keys_scanned, fallback_post_access_rows)) =
                    Self::try_materialize_load_via_row_collector(
                        inputs.ctx,
                        inputs.plan,
                        cursor_boundary,
                        fallback_resolved.key_stream.as_mut(),
                    )?
                {
                    (
                        fallback_page,
                        fallback_keys_scanned,
                        fallback_post_access_rows,
                    )
                } else {
                    LoadExecutor::<E>::materialize_key_stream_into_page(
                        inputs.ctx,
                        inputs.plan,
                        inputs.predicate_slots,
                        fallback_resolved.key_stream.as_mut(),
                        fallback_route_plan.scan_hints.load_scan_budget_hint,
                        fallback_route_plan.streaming_access_shape_safe(),
                        cursor_boundary,
                        fallback_route_plan.direction(),
                        continuation_signature,
                    )?
                };
            let fallback_rows_scanned = fallback_resolved
                .rows_scanned_override
                .unwrap_or(fallback_keys_scanned);
            let fallback_distinct_keys_deduped = fallback_resolved
                .distinct_keys_deduped_counter
                .as_ref()
                .map_or(0, |counter| counter.get());

            // Retry accounting keeps observability faithful to actual work.
            rows_scanned = rows_scanned.saturating_add(fallback_rows_scanned);
            optimization = fallback_resolved.optimization;
            index_predicate_applied =
                index_predicate_applied || fallback_resolved.index_predicate_applied;
            index_predicate_keys_rejected = index_predicate_keys_rejected
                .saturating_add(fallback_resolved.index_predicate_keys_rejected);
            distinct_keys_deduped =
                distinct_keys_deduped.saturating_add(fallback_distinct_keys_deduped);
            page = fallback_page;
            post_access_rows = fallback_post_access_rows;
        }

        Ok(MaterializedExecutionAttempt {
            page,
            rows_scanned,
            post_access_rows,
            optimization,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        })
    }

    // Retry index-range limit pushdown when a bounded residual-filter pass may
    // have under-filled the requested page window.
    fn index_range_limited_residual_retry_required<K>(
        plan: &AccessPlannedQuery<K>,
        cursor_boundary: Option<&CursorBoundary>,
        route_plan: &ExecutionPlan,
        rows_scanned: usize,
        post_access_rows: usize,
    ) -> bool {
        let Some(limit_spec) = route_plan.index_range_limit_spec else {
            return false;
        };
        if plan.predicate.is_none() {
            return false;
        }
        if limit_spec.fetch == 0 {
            return false;
        }
        let Some(limit) = plan.page.as_ref().and_then(|page| page.limit) else {
            return false;
        };
        let keep_count = Self::effective_keep_count_for_limit(plan, cursor_boundary, limit);
        if keep_count == 0 {
            return false;
        }
        if rows_scanned < limit_spec.fetch {
            return false;
        }

        post_access_rows < keep_count
    }
}
