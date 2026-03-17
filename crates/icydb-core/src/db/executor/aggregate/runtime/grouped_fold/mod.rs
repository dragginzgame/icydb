//! Module: executor::aggregate::runtime::grouped_fold
//! Responsibility: grouped key-stream construction and fold execution mechanics.
//! Does not own: grouped route derivation or grouped output finalization.
//! Boundary: consumes grouped route-stage payload and emits grouped fold-stage payload.

mod candidate_rows;
mod engine_init;
mod global_distinct;
mod ingest;
mod page_finalize;

use crate::{
    db::{
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionPreparation,
            aggregate::GroupError,
            aggregate::runtime::grouped_fold::{
                candidate_rows::collect_grouped_candidate_rows,
                page_finalize::finalize_grouped_page,
            },
            group::{grouped_budget_observability, grouped_execution_context_from_planner_config},
            pipeline::contracts::{
                ExecutionInputs, GroupedCursorPage, GroupedFoldStage, GroupedRouteStageProjection,
                GroupedStreamStage, LoadExecutor,
            },
            plan_metrics::record_grouped_plan_metrics,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Build one grouped key stream from route-owned grouped execution metadata.
    pub(in crate::db::executor) fn build_grouped_stream<'a, R>(
        &'a self,
        route: &'a R,
    ) -> Result<GroupedStreamStage<'a, E>, InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(route.plan());
        let ctx = self.db.recovered_context::<E>()?;
        let execution_inputs = ExecutionInputs::new(
            &ctx,
            route.plan(),
            AccessStreamBindings {
                index_prefix_specs: route.index_prefix_specs(),
                index_range_specs: route.index_range_specs(),
                continuation: AccessScanContinuationInput::new(None, route.direction()),
            },
            &execution_preparation,
        );
        record_grouped_plan_metrics(&route.plan().access, route.grouped_plan_metrics_strategy());
        let resolved = Self::resolve_execution_key_stream_without_distinct(
            &execution_inputs,
            route.grouped_route_plan(),
            IndexCompilePolicy::ConservativeSubset,
        )?;

        Ok(GroupedStreamStage::new(
            ctx,
            execution_preparation,
            resolved,
        ))
    }

    // Execute grouped folding over one resolved grouped key stream.
    pub(in crate::db::executor) fn execute_group_fold<R>(
        route: &R,
        mut stream: GroupedStreamStage<'_, E>,
    ) -> Result<GroupedFoldStage, InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        // Phase 1: initialize grouped fold context, projection contracts, and reducers.
        let mut grouped_execution_context =
            grouped_execution_context_from_planner_config(Some(route.grouped_execution()));
        let max_groups_bound =
            usize::try_from(grouped_execution_context.config().max_groups()).unwrap_or(usize::MAX);
        let grouped_budget = grouped_budget_observability(&grouped_execution_context);
        debug_assert!(
            grouped_budget.max_groups() >= grouped_budget.groups()
                && grouped_budget.max_group_bytes() >= grouped_budget.estimated_bytes()
                && grouped_execution_context
                    .config()
                    .max_distinct_values_total()
                    >= grouped_budget.distinct_values()
                && grouped_budget.aggregate_states() >= grouped_budget.groups(),
            "grouped budget observability invariants must hold at grouped route entry",
        );
        let aggregate_count = route.projection_layout().aggregate_positions().len();
        let grouped_projection_spec = route.plan().projection_spec(E::MODEL);
        let (mut grouped_engines, mut short_circuit_keys) =
            Self::build_grouped_engines(route, &grouped_execution_context)?;

        // Phase 2: run global DISTINCT grouped fast path when route contracts permit it.
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        if let Some(global_distinct_result) = Self::try_execute_global_distinct_fold(
            route,
            &mut stream,
            &mut grouped_execution_context,
            &grouped_projection_spec,
            &mut scanned_rows,
            &mut filtered_rows,
        )? {
            return Ok(global_distinct_result);
        }

        // Phase 3: ingest grouped rows into per-aggregate reducers.
        (scanned_rows, filtered_rows) = Self::ingest_grouped_rows_into_engines(
            route,
            &mut stream,
            &mut grouped_execution_context,
            grouped_engines.as_mut_slice(),
            short_circuit_keys.as_mut_slice(),
            max_groups_bound,
        )?;

        // Phase 4: finalize reducer outputs into sorted grouped candidate rows.
        let grouped_pagination_window = route.grouped_pagination_window().clone();
        let grouped_candidate_rows = collect_grouped_candidate_rows(
            route,
            grouped_engines,
            aggregate_count,
            max_groups_bound,
            &grouped_pagination_window,
        )?;

        // Phase 5: page finalized candidates and project grouped outputs.
        let (page_rows, next_cursor) = finalize_grouped_page(
            route,
            &grouped_projection_spec,
            grouped_candidate_rows,
            &grouped_pagination_window,
        )?;
        Ok(GroupedFoldStage::from_grouped_stream(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            filtered_rows,
            true,
            &stream,
            scanned_rows,
        ))
    }

    // Map grouped reducer errors into executor-owned error classes.
    pub(super) fn map_group_error(err: GroupError) -> InternalError {
        match err {
            GroupError::MemoryLimitExceeded { .. } | GroupError::DistinctBudgetExceeded { .. } => {
                crate::db::error::executor_internal(err.to_string())
            }
            GroupError::Internal(inner) => inner,
        }
    }
}
