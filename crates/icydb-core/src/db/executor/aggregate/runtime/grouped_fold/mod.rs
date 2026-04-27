//! Module: executor::aggregate::runtime::grouped_fold
//! Responsibility: grouped key-stream construction and fold execution mechanics.
//! Does not own: grouped route derivation or grouped output finalization.
//! Boundary: consumes grouped route-stage payload and emits grouped fold-stage payload.

mod bundle;
mod count;
mod dispatch;
mod generic;
mod metrics;
mod page_finalize;
mod utils;

use crate::{
    db::{
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutionPreparation,
            aggregate::{
                GroupError,
                runtime::{
                    grouped_distinct::{
                        execute_global_distinct_field_aggregate, page_global_distinct_grouped_row,
                    },
                    grouped_fold::{
                        count::execute_single_grouped_count_fold_stage,
                        dispatch::GroupedFoldRouteKind,
                        generic::execute_generic_grouped_fold_stage,
                    },
                    grouped_output::project_grouped_rows_from_projection,
                },
            },
            group::grouped_budget_observability,
            group::grouped_execution_context_from_planner_config,
            pipeline::{
                contracts::{
                    ExecutionInputs, ExecutionRuntimeAdapter, GroupedCursorPage, GroupedRouteStage,
                    PreparedExecutionInputParts, PreparedExecutionProjection,
                    ProjectionMaterializationMode,
                },
                runtime::{
                    ExecutionAttemptKernel, GroupedFoldStage, GroupedStreamStage,
                    StructuralGroupedRowRuntime,
                },
            },
            plan_metrics::record_grouped_plan_metrics,
        },
        index::IndexCompilePolicy,
    },
    error::InternalError,
};
#[cfg(feature = "diagnostics")]
pub(crate) use metrics::{GroupedCountFoldMetrics, with_grouped_count_fold_metrics};

// Build one grouped key stream from route-owned grouped execution metadata
// using already-resolved runtime and row-decode boundaries.
pub(in crate::db::executor) fn build_grouped_stream_with_runtime(
    route: &GroupedRouteStage,
    runtime: &ExecutionRuntimeAdapter,
    execution_preparation: ExecutionPreparation,
    row_runtime: StructuralGroupedRowRuntime,
) -> Result<GroupedStreamStage, InternalError> {
    let execution_inputs = ExecutionInputs::new_prepared(PreparedExecutionInputParts {
        runtime,
        plan: route.plan(),
        executable_access: route.plan().access.executable_contract(),
        stream_bindings: AccessStreamBindings {
            index_prefix_specs: route.index_prefix_specs(),
            index_range_specs: route.index_range_specs(),
            continuation: AccessScanContinuationInput::new(None, route.direction()),
        },
        execution_preparation: &execution_preparation,
        projection_materialization: ProjectionMaterializationMode::SharedValidation,
        prepared_projection: PreparedExecutionProjection::empty(),
        emit_cursor: true,
    });
    record_grouped_plan_metrics(&route.plan().access, route.grouped_execution_mode());
    let resolved = ExecutionAttemptKernel::new(&execution_inputs)
        .resolve_execution_key_stream_without_distinct(
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
    mut stream: GroupedStreamStage,
) -> Result<GroupedFoldStage, InternalError> {
    // Phase 1: initialize grouped fold context, projection contracts, and reducers.
    let mut grouped_execution_context =
        grouped_execution_context_from_planner_config(Some(route.grouped_execution()));
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
    let grouped_projection_spec = route.plan().frozen_projection_spec().clone();

    // Phase 2: dispatch grouped fold execution through one route-owned mode
    // selector so DISTINCT, dedicated COUNT(*), and generic grouped reduce
    // paths do not re-derive the same specialization policy independently.
    match GroupedFoldRouteKind::for_route(route) {
        GroupedFoldRouteKind::GlobalDistinct => execute_global_distinct_grouped_fold_stage(
            route,
            &mut stream,
            &mut grouped_execution_context,
            &grouped_projection_spec,
        ),
        GroupedFoldRouteKind::CountRowsDedicated => execute_single_grouped_count_fold_stage(
            route,
            &mut stream,
            &mut grouped_execution_context,
            &grouped_projection_spec,
        ),
        GroupedFoldRouteKind::Generic => execute_generic_grouped_fold_stage(
            route,
            &mut stream,
            &mut grouped_execution_context,
            &grouped_projection_spec,
        ),
    }
}

// Execute one grouped global-DISTINCT route through the dedicated grouped
// distinct aggregate path selected by the grouped fold route kind.
fn execute_global_distinct_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut crate::db::executor::aggregate::ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    grouped_execution_context
        .record_implicit_single_group()
        .map_err(GroupError::into_internal_error)?;
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let global_row = execute_global_distinct_field_aggregate(
        route.consistency(),
        row_runtime,
        resolved,
        effective_runtime_filter_program,
        grouped_execution_context,
        route.grouped_distinct_execution_strategy(),
        (&mut scanned_rows, &mut filtered_rows),
    )?;
    let grouped_window = route.grouped_pagination_window();
    let page_rows = page_global_distinct_grouped_row(
        global_row,
        grouped_window.initial_offset_for_page(),
        grouped_window.limit(),
    );
    metrics::record_projection_rows_input(page_rows.len());
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;

    Ok(GroupedFoldStage::from_grouped_stream(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor: None,
        },
        filtered_rows,
        false,
        stream,
        scanned_rows,
    ))
}
