//! Module: executor::aggregate::runtime::grouped_fold::count
//! Responsibility: dedicated grouped `COUNT(*)` fold execution.
//! Boundary: owns count-state ingestion, windowing, and finalization.

mod finalize;
mod ingest;
mod state;
#[cfg(test)]
mod tests;
mod window;

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext,
            runtime::grouped_fold::{
                count::{finalize::finalize_grouped_count_page, state::GroupedCountState},
                dispatch::{GroupedCountKeyPath, GroupedCountProbeKind},
                metrics,
                utils::group_capacity_hint,
            },
        },
        pipeline::{
            contracts::GroupedRouteStage,
            contracts::ResolvedExecutionKeyStream,
            runtime::{GroupedFoldStage, GroupedStreamStage, RowView, StructuralGroupedRowRuntime},
        },
    },
    error::InternalError,
};

pub(in crate::db::executor::aggregate::runtime::grouped_fold) use ingest::materialize_group_key_from_row_view;

// Execute grouped `COUNT(*)` through a dedicated fold path that keeps only one
// canonical grouped-count map instead of the generic grouped reducer stack.
pub(super) fn execute_single_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    metrics::record_fold_stage_run();
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let consistency = route.consistency();
    let key_path = GroupedCountKeyPath::for_route(route, effective_runtime_filter_program);
    let group_capacity_hint = group_capacity_hint(
        resolved.cheap_access_candidate_count_hint(),
        grouped_execution_context.config().max_groups(),
    );
    let mut grouped_counts = GroupedCountState::with_capacity(group_capacity_hint);
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;

    // Phase 1: fold grouped source rows directly into one canonical count map.
    match key_path {
        GroupedCountKeyPath::DirectSingleField { group_field_index } => {
            while let Some(data_key) = resolved.key_stream_mut().next_key()? {
                let (row_materialization_local_instructions, group_value) =
                    metrics::measure(|| {
                        row_runtime.read_single_group_value(
                            consistency,
                            &data_key,
                            group_field_index,
                        )
                    });
                metrics::record_row_materialization(row_materialization_local_instructions);
                let Some(group_value) = group_value? else {
                    continue;
                };
                scanned_rows = scanned_rows.saturating_add(1);
                filtered_rows = filtered_rows.saturating_add(1);
                grouped_counts
                    .increment_single_group_value(group_value, grouped_execution_context)?;
            }
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Borrowed,
        } => {
            fold_row_view_count_rows(
                route,
                row_runtime,
                resolved,
                effective_runtime_filter_program,
                grouped_execution_context,
                &mut grouped_counts,
                (&mut scanned_rows, &mut filtered_rows),
                GroupedCountState::increment_row_borrowed_group_probe,
            )?;
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Owned,
        } => {
            fold_row_view_count_rows(
                route,
                row_runtime,
                resolved,
                effective_runtime_filter_program,
                grouped_execution_context,
                &mut grouped_counts,
                (&mut scanned_rows, &mut filtered_rows),
                GroupedCountState::increment_row_owned_group_key,
            )?;
        }
    }

    // Phase 2: page and project the finalized grouped-count rows directly so
    // this dedicated path does not round-trip through the generic candidate
    // row envelope only to rebuild grouped rows immediately afterwards.
    let (page_rows, next_cursor) =
        finalize_grouped_count_page(route, grouped_projection_spec, grouped_counts.into_groups())?;

    Ok(GroupedFoldStage::from_grouped_stream(
        crate::db::executor::pipeline::contracts::GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Fold row-view grouped-count input through one statically selected ingest
// function so borrowed and owned paths are resolved before the source-row loop.
#[expect(
    clippy::too_many_arguments,
    reason = "the helper preserves the pre-existing hot-loop data flow while avoiding dynamic dispatch"
)]
fn fold_row_view_count_rows(
    route: &GroupedRouteStage,
    row_runtime: &StructuralGroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    effective_runtime_filter_program: Option<
        &crate::db::query::plan::EffectiveRuntimeFilterProgram,
    >,
    grouped_execution_context: &mut ExecutionContext,
    grouped_counts: &mut GroupedCountState,
    counters: (&mut usize, &mut usize),
    mut increment_row: impl FnMut(
        &mut GroupedCountState,
        &RowView,
        &[crate::db::query::plan::FieldSlot],
        &mut ExecutionContext,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let consistency = route.consistency();
    let (scanned_rows, filtered_rows) = counters;

    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        let (row_materialization_local_instructions, row_view) =
            metrics::measure(|| row_runtime.read_row_view(consistency, &data_key));
        metrics::record_row_materialization(row_materialization_local_instructions);
        let Some(row_view) = row_view? else {
            continue;
        };
        *scanned_rows = scanned_rows.saturating_add(1);
        if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
            && !row_view.eval_filter_program(effective_runtime_filter_program)?
        {
            continue;
        }
        *filtered_rows = filtered_rows.saturating_add(1);
        increment_row(
            grouped_counts,
            &row_view,
            route.group_fields(),
            grouped_execution_context,
        )?;
    }

    Ok(())
}
