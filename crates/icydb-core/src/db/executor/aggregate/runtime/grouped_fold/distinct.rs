//! Module: executor::aggregate::runtime::grouped_fold::distinct
//! Responsibility: grouped fold orchestration for global DISTINCT aggregates.
//! Boundary: keeps DISTINCT route wiring separate from generic and count folds.

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext, GroupError, ProjectionSpec,
            runtime::{
                grouped_distinct::{
                    execute_global_distinct_field_aggregate, page_global_distinct_grouped_row,
                },
                grouped_output::project_grouped_rows_from_projection,
            },
        },
        pipeline::{
            contracts::{GroupedCursorPage, GroupedRouteStage},
            runtime::GroupedStreamStage,
        },
    },
    error::InternalError,
};

// Execute one grouped global-DISTINCT route through the dedicated grouped
// distinct aggregate path selected by the grouped fold route kind.
pub(super) fn execute_global_distinct_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedCursorPage, InternalError> {
    grouped_execution_context
        .record_implicit_single_group()
        .map_err(GroupError::into_internal_error)?;
    let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let global_row = execute_global_distinct_field_aggregate(
        route.consistency(),
        row_runtime,
        resolved,
        effective_runtime_filter_program,
        grouped_execution_context,
        route.grouped_distinct_execution_strategy(),
    )?;
    let grouped_window = route.grouped_pagination_window();
    let page_rows = page_global_distinct_grouped_row(
        global_row,
        grouped_window.initial_offset_for_page(),
        grouped_window.limit(),
    );
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_is_identity(),
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;

    // The implicit global group may produce a row even when no input matched.
    Ok(GroupedCursorPage {
        rows: page_rows,
        next_cursor: None,
    })
}
