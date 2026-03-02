//! Module: executor::load::entrypoints
//! Responsibility: load executor public entrypoint orchestration for scalar and grouped paths.
//! Does not own: stream resolution internals or projection/having evaluation mechanics.
//! Boundary: validates entrypoint contracts, builds route context, and delegates execution.

use crate::{
    db::{
        cursor::{GroupedPlannedCursor, PlannedCursor},
        executor::{
            AccessStreamBindings, ContinuationEngine, ExecutablePlan, ExecutionKernel,
            ExecutionPreparation, ExecutionTrace,
            load::{CursorPage, GroupedCursorPage, LoadExecutor},
            plan_metrics::record_plan_metrics,
            range_token_anchor_key, validate_executor_plan,
        },
        index::IndexCompilePolicy,
        query::plan::LogicalPlan,
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
};

// Cursor variant input contract for unified load entrypoint dispatch.
enum LoadCursorInput {
    Scalar(PlannedCursor),
    Grouped(GroupedPlannedCursor),
}

// Unified load entrypoint output contract spanning scalar and grouped payloads.
enum LoadExecutionPage<E: EntityKind> {
    Scalar(CursorPageWithTrace<E>),
    Grouped(GroupedPageWithTrace),
}

type CursorPageWithTrace<E> = (CursorPage<E>, Option<ExecutionTrace>);
type GroupedPageWithTrace = (GroupedCursorPage, Option<ExecutionTrace>);

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
        let execution =
            self.execute_paged_internal_traced(plan, LoadCursorInput::Scalar(cursor.into()))?;

        let LoadExecutionPage::Scalar(page) = execution else {
            return Err(super::invariant(
                "scalar load entrypoint must emit scalar execution payload",
            ));
        };

        Ok(page)
    }

    // Execute one grouped load plan with grouped cursor support and trace output.
    pub(in crate::db) fn execute_grouped_paged_with_cursor_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: impl Into<GroupedPlannedCursor>,
    ) -> Result<(GroupedCursorPage, Option<ExecutionTrace>), InternalError> {
        let execution =
            self.execute_paged_internal_traced(plan, LoadCursorInput::Grouped(cursor.into()))?;

        let LoadExecutionPage::Grouped(page) = execution else {
            return Err(super::invariant(
                "grouped load entrypoint must emit grouped execution payload",
            ));
        };

        Ok(page)
    }

    // Unified load entrypoint pipeline:
    // 1) validate mode and logical/cursor shape pairing
    // 2) revalidate cursor through continuation protocol boundary
    // 3) dispatch to scalar or grouped execution spine
    fn execute_paged_internal_traced(
        &self,
        plan: ExecutablePlan<E>,
        cursor: LoadCursorInput,
    ) -> Result<LoadExecutionPage<E>, InternalError> {
        if !plan.mode().is_load() {
            return Err(super::invariant("load executor requires load plans"));
        }

        let grouped_plan = matches!(&plan.as_inner().logical, LogicalPlan::Grouped(_));
        match cursor {
            LoadCursorInput::Scalar(cursor) => {
                if grouped_plan {
                    return Err(super::invariant(
                        "grouped plans require execute_grouped pagination entrypoints",
                    ));
                }
                let cursor = plan.revalidate_cursor(cursor)?;
                let page = self.execute_scalar_path(plan, cursor)?;

                Ok(LoadExecutionPage::Scalar(page))
            }
            LoadCursorInput::Grouped(cursor) => {
                if !grouped_plan {
                    return Err(super::invariant(
                        "grouped execution requires grouped logical plans",
                    ));
                }
                let cursor = plan.revalidate_grouped_cursor(cursor)?;
                let page = self.execute_grouped_path(plan, cursor)?;

                Ok(LoadExecutionPage::Grouped(page))
            }
        }
    }

    // Scalar execution spine:
    // 1) normalize continuation runtime bindings
    // 2) derive routing and trace contracts
    // 3) execute kernel materialization
    // 4) finalize scalar page + observability
    fn execute_scalar_path(
        &self,
        plan: ExecutablePlan<E>,
        cursor: PlannedCursor,
    ) -> Result<(CursorPage<E>, Option<ExecutionTrace>), InternalError> {
        let scalar_runtime = ContinuationEngine::scalar_runtime(cursor);
        let cursor_boundary = scalar_runtime.cursor_boundary();
        let index_range_token = scalar_runtime.index_range_token();
        let continuation_signature = plan.continuation_signature();
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let route_plan = Self::build_execution_route_plan_for_load(
            plan.as_inner(),
            cursor_boundary,
            index_range_token,
            None,
        )?;
        let continuation_applied = !matches!(
            route_plan.continuation_mode(),
            crate::db::executor::route::ContinuationMode::Initial
        );
        let direction = route_plan.direction();
        debug_assert_eq!(
            route_plan.window().effective_offset,
            ExecutionKernel::effective_page_offset(plan.as_inner(), cursor_boundary),
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
                    index_range_anchor: index_range_token.map(range_token_anchor_key),
                    direction,
                },
                execution_preparation: &execution_preparation,
            };

            record_plan_metrics(&plan.access);
            let materialized = ExecutionKernel::materialize_with_optional_residual_retry(
                &execution_inputs,
                &route_plan,
                cursor_boundary,
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
