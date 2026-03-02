//! Module: executor::load::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            AccessStreamBindings, ExecutablePlan, ExecutionKernel, ExecutionPreparation,
            ExecutionTrace,
            load::{CursorPage, GroupedCursorPage, LoadExecutor},
            plan_metrics::record_plan_metrics,
            range_token_anchor_key, range_token_from_cursor_anchor, validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::plan::LogicalPlan,
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one scalar load plan without explicit cursor input.
    pub(crate) fn execute(&self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        self.execute_paged_with_cursor(plan, PlannedCursor::none())
            .map(|page| page.items)
    }

    // Execute one scalar load plan with optional cursor input.
    pub(in crate::db) fn execute_paged_with_cursor(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<CursorPage<E>, InternalError> {
        self.execute_paged_with_cursor_traced(plan, cursor)
            .map(|(page, _)| page)
    }

    // Execute one scalar load plan and optionally emit execution trace output.
    pub(in crate::db) fn execute_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<PlannedCursor>,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        if matches!(&plan.as_inner().logical, LogicalPlan::Grouped(_)) {
            return Err(super::invariant(
                "grouped plans require execute_grouped pagination entrypoints",
            ));
        }

        let cursor: PlannedCursor = plan.revalidate_cursor(cursor.into())?;
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_cursor_anchor);

        if !plan.mode().is_load() {
            return Err(super::invariant("load executor requires load plans"));
        }

        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let route_plan = Self::build_execution_route_plan_for_load(
            plan.as_inner(),
            cursor_boundary.as_ref(),
            index_range_token.as_ref(),
            None,
        )?;
        let continuation_applied = !matches!(
            route_plan.continuation_mode(),
            crate::db::executor::route::ContinuationMode::Initial
        );
        let direction = route_plan.direction();
        debug_assert_eq!(
            route_plan.window().effective_offset,
            ExecutionKernel::effective_page_offset(plan.as_inner(), cursor_boundary.as_ref()),
            "route window effective offset must match logical plan offset semantics",
        );
        let mut execution_trace = self
            .debug
            .then(|| ExecutionTrace::new(plan.access(), direction, continuation_applied));
        let plan = plan.into_inner();
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&plan);

        let result = (|| {
            let mut span = Span::<E>::new(ExecKind::Load);

            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;
            let execution_inputs = super::ExecutionInputs {
                ctx: &ctx,
                plan: &plan,
                stream_bindings: AccessStreamBindings {
                    index_prefix_specs: index_prefix_specs.as_slice(),
                    index_range_specs: index_range_specs.as_slice(),
                    index_range_anchor: index_range_token.as_ref().map(range_token_anchor_key),
                    direction,
                },
                execution_preparation: &execution_preparation,
            };

            record_plan_metrics(&plan.access);
            // Plan execution routing once, then execute in canonical order.
            // Resolve one canonical key stream, then run shared page materialization/finalization.
            let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
                &execution_inputs,
                &route_plan,
                cursor_boundary.as_ref(),
                continuation_signature,
                IndexCompilePolicy::ConservativeSubset,
            )?;
            let page = materialized.page;
            let rows_scanned = materialized.rows_scanned;
            let post_access_rows = materialized.post_access_rows;
            let optimization = materialized.optimization;
            let index_predicate_applied = materialized.index_predicate_applied;
            let index_predicate_keys_rejected = materialized.index_predicate_keys_rejected;
            let distinct_keys_deduped = materialized.distinct_keys_deduped;

            Ok(Self::finalize_execution(
                page,
                optimization,
                rows_scanned,
                post_access_rows,
                index_predicate_applied,
                index_predicate_keys_rejected,
                distinct_keys_deduped,
                &mut span,
                &mut execution_trace,
            ))
        })();

        result.map(|page| (page, execution_trace))
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        if !matches!(&plan.as_inner().logical, LogicalPlan::Grouped(_)) {
            return Err(super::invariant(
                "grouped execution requires grouped logical plans",
            ));
        }

        let cursor = plan.revalidate_grouped_cursor(cursor.into())?;

        self.execute_grouped_path(plan, cursor)
    }

    // Grouped execution spine:
    // 1) resolve grouped route/metadata
    // 2) build grouped key stream
    // 3) execute grouped fold
    // 4) finalize grouped output + observability
    fn execute_grouped_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: GroupedPlannedCursor,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let route = Self::resolve_grouped_route(plan, cursor, self.debug)?;
        let stream = self.build_grouped_stream(&route)?;
        let folded = Self::execute_group_fold(&route, stream)?;

        Ok(Self::finalize_grouped_output(route, folded))
    }
}
