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
            ExecutionContext, GroupError, ProjectionSpec,
            runtime::grouped_fold::{
                bundle::OrderedGroupFoldState,
                count::{
                    finalize::finalize_grouped_count_page, ingest::fold_row_view_count_rows,
                    state::GroupedCountState,
                },
                dispatch::{GroupedCountKeyPath, GroupedCountProbeKind},
                generic::OrderedGroupedPageSelection,
                metrics,
                utils::group_capacity_hint,
            },
            value_reducer::finalize_count,
        },
        group::{GroupKey, KeyCanonicalError},
        pipeline::{
            contracts::{GroupedCursorPage, GroupedRouteStage},
            runtime::{GroupedFoldStage, GroupedStreamStage},
        },
        route::GroupedExecutionMode,
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
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    metrics::record_fold_stage_run();
    if matches!(
        route.grouped_execution_mode()?,
        GroupedExecutionMode::OrderedStreaming
    ) {
        return execute_ordered_grouped_count_fold_stage(
            route,
            stream,
            grouped_execution_context,
            grouped_projection_spec,
        );
    }

    let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
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
        #[cfg(feature = "diagnostics")]
        grouped_execution_context.successful_runtime_stats(false),
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Execute dedicated grouped COUNT(*) through the shared ordered transition
// owner while retaining the direct single-field key extraction hot path.
fn execute_ordered_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let mut transitions = OrderedGroupFoldState::<u32>::new(1);
    let mut selection = OrderedGroupedPageSelection::new(route, grouped_projection_spec, 1)?;
    let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let consistency = route.consistency();
    let key_path = GroupedCountKeyPath::for_route(route, effective_runtime_filter_program);
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let mut finalized_groups = 0usize;
    let mut early_scan_stop = false;

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
                metrics::record_borrowed_probe_row();
                metrics::record_owned_key_materialization();
                let group_key = GroupKey::from_single_canonical_group_value(group_value)
                    .map_err(KeyCanonicalError::into_internal_error)?;
                early_scan_stop = apply_ordered_count_row(
                    &mut transitions,
                    grouped_execution_context,
                    route,
                    group_key,
                    &mut selection,
                    &mut finalized_groups,
                )?;
                if early_scan_stop {
                    break;
                }
                filtered_rows = filtered_rows.saturating_add(1);
            }
        }
        GroupedCountKeyPath::RowView { .. } => {
            while let Some(data_key) = resolved.key_stream_mut().next_key()? {
                let (row_materialization_local_instructions, row_view) =
                    metrics::measure(|| row_runtime.read_row_view(consistency, &data_key));
                metrics::record_row_materialization(row_materialization_local_instructions);
                let Some(row_view) = row_view? else {
                    continue;
                };
                scanned_rows = scanned_rows.saturating_add(1);
                if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                    && !row_view.eval_filter_program(effective_runtime_filter_program)?
                {
                    continue;
                }
                metrics::record_owned_group_fallback_row();
                let group_key =
                    materialize_group_key_from_row_view(&row_view, route.group_fields(), None)?;
                early_scan_stop = apply_ordered_count_row(
                    &mut transitions,
                    grouped_execution_context,
                    route,
                    group_key,
                    &mut selection,
                    &mut finalized_groups,
                )?;
                if early_scan_stop {
                    break;
                }
                filtered_rows = filtered_rows.saturating_add(1);
            }
        }
    }

    if !early_scan_stop {
        transitions
            .finish(grouped_execution_context, |group_key, count| {
                finalized_groups = finalized_groups.saturating_add(1);
                selection.push_finalized_values(group_key, vec![finalize_count(u64::from(count))])
            })
            .map_err(GroupError::into_internal_error)?;
    }
    metrics::record_finalize_stage(finalized_groups);
    let (page_rows, next_cursor) = selection.finish(route)?;

    Ok(GroupedFoldStage::from_grouped_stream(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        #[cfg(feature = "diagnostics")]
        grouped_execution_context.successful_runtime_stats(early_scan_stop),
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Fold one dedicated count row through the shared ordered transition state
// and canonical incremental grouped-page selector.
fn apply_ordered_count_row(
    transitions: &mut OrderedGroupFoldState<u32>,
    grouped_execution_context: &mut ExecutionContext,
    route: &GroupedRouteStage,
    group_key: GroupKey,
    selection: &mut OrderedGroupedPageSelection<'_>,
    finalized_groups: &mut usize,
) -> Result<bool, InternalError> {
    transitions
        .apply_row(
            grouped_execution_context,
            group_key,
            route.direction(),
            || {
                metrics::record_new_group_insert(0);
                0
            },
            |count, _context| {
                metrics::record_rows_folded();
                if *count > 0 {
                    metrics::record_existing_group_hit(0);
                }
                *count = count.saturating_add(1);
                Ok(())
            },
            |closed_key, count| {
                *finalized_groups = finalized_groups.saturating_add(1);
                selection.push_finalized_values(closed_key, vec![finalize_count(u64::from(count))])
            },
        )
        .map_err(GroupError::into_internal_error)
}
