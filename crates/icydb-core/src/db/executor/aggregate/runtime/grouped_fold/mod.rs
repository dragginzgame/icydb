//! Module: executor::aggregate::runtime::grouped_fold
//! Responsibility: grouped key-stream construction and fold execution mechanics.
//! Does not own: grouped route derivation or grouped output finalization.
//! Boundary: consumes grouped route-stage payload and emits grouped fold-stage payload.

mod candidate_rows;
mod engine_init;
mod ingest;
mod page_finalize;

use crate::{
    db::{
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionKernel,
            ExecutionPreparation,
            aggregate::runtime::grouped_fold::{
                candidate_rows::collect_grouped_candidate_rows, engine_init::build_grouped_engines,
                ingest::fold_group_rows_into_engines, page_finalize::finalize_grouped_page,
            },
            aggregate::{
                GroupError,
                runtime::{
                    grouped_distinct::{
                        GlobalDistinctFieldExecutionSpec, execute_global_distinct_field_aggregate,
                        global_distinct_field_execution_spec, page_global_distinct_grouped_row,
                    },
                    grouped_output::project_grouped_rows_from_projection,
                },
            },
            group::{grouped_budget_observability, grouped_execution_context_from_planner_config},
            pipeline::contracts::{
                ExecutionInputs, ExecutionRuntime, GroupedCursorPage, GroupedFoldStage,
                GroupedRouteStage, GroupedRowRuntime, GroupedStreamStage,
            },
            plan_metrics::record_grouped_plan_metrics,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
    model::entity::EntityModel,
};

// Build one grouped key stream from route-owned grouped execution metadata
// using already-resolved runtime and row-decode boundaries.
pub(in crate::db::executor) fn build_grouped_stream_with_runtime<'a>(
    route: &GroupedRouteStage,
    runtime: &dyn ExecutionRuntime,
    entity_model: &'static EntityModel,
    slot_map: Option<Vec<usize>>,
    row_runtime: Box<dyn GroupedRowRuntime + 'a>,
) -> Result<GroupedStreamStage<'a>, InternalError> {
    let execution_preparation =
        ExecutionPreparation::from_plan(entity_model, route.plan(), slot_map);
    let execution_inputs = ExecutionInputs::new(
        runtime,
        route.plan(),
        AccessStreamBindings {
            index_prefix_specs: route.index_prefix_specs(),
            index_range_specs: route.index_range_specs(),
            continuation: AccessScanContinuationInput::new(None, route.direction()),
        },
        &execution_preparation,
    );
    record_grouped_plan_metrics(&route.plan().access, route.grouped_plan_metrics_strategy());
    let resolved = ExecutionKernel::resolve_execution_key_stream_without_distinct(
        &execution_inputs,
        route.grouped_route_plan(),
        IndexCompilePolicy::ConservativeSubset,
    )?;

    Ok(GroupedStreamStage::new(
        row_runtime,
        execution_preparation,
        resolved,
    ))
}

// Execute grouped aggregate folding over one resolved grouped key stream using
// only structural grouped reducer/runtime contracts.
pub(in crate::db::executor) fn execute_group_fold_stage(
    route: &GroupedRouteStage,
    mut stream: GroupedStreamStage<'_>,
) -> Result<GroupedFoldStage, InternalError> {
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
    let grouped_projection_spec = route.plan().projection_spec(route.entity_model());
    let (mut grouped_engines, mut short_circuit_keys) =
        build_grouped_engines(route, &grouped_execution_context)?;

    // Phase 2: route one global DISTINCT grouped aggregate through the
    // canonical grouped aggregate entrypoint when strategy permits it.
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    if let Some(GlobalDistinctFieldExecutionSpec {
        aggregate_kind,
        target_field,
    }) = global_distinct_field_execution_spec(route.grouped_distinct_execution_strategy())
    {
        grouped_execution_context
            .record_implicit_single_group()
            .map_err(GroupError::into_internal_error)?;
        let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
        let compiled_predicate = execution_preparation.compiled_predicate();
        let global_row = execute_global_distinct_field_aggregate(
            route.consistency(),
            row_runtime,
            resolved,
            compiled_predicate,
            &mut grouped_execution_context,
            route.entity_model(),
            (target_field, aggregate_kind),
            (&mut scanned_rows, &mut filtered_rows),
        )?;
        let grouped_window = route.grouped_pagination_window();
        let page_rows = page_global_distinct_grouped_row(
            global_row,
            grouped_window.initial_offset_for_page(),
            grouped_window.limit(),
        );
        let page_rows = project_grouped_rows_from_projection(
            &grouped_projection_spec,
            route.projection_layout(),
            route.group_fields(),
            route.grouped_aggregate_exprs(),
            page_rows,
        )?;

        return Ok(GroupedFoldStage::from_grouped_stream(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor: None,
            },
            filtered_rows,
            false,
            &stream,
            scanned_rows,
        ));
    }

    // Phase 3: ingest grouped rows into per-aggregate reducers.
    (scanned_rows, filtered_rows) = fold_group_rows_into_engines(
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
