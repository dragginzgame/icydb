//! Module: executor::aggregate::runtime::grouped_fold::count
//! Responsibility: dedicated grouped `COUNT(*)` fold execution.
//! Boundary: owns count-state ingestion, windowing, and finalization.

mod finalize;
mod ingest;
mod metadata;
mod state;
#[cfg(test)]
mod tests;
mod window;

use crate::{
    db::{
        executor::{
            aggregate::{
                ExecutionContext, GroupError, ProjectionSpec,
                runtime::grouped_fold::{
                    bundle::OrderedGroupFoldState,
                    count::{
                        finalize::finalize_grouped_count_page,
                        ingest::fold_single_field_count_rows, state::GroupedCountState,
                    },
                    dispatch::{GroupedCountKeyPath, GroupedCountProbeKind},
                    generic::OrderedGroupedPageSelection,
                    utils::resolve_group_field_value,
                },
                runtime::{
                    fold_filtered_source_rows, fold_filtered_source_rows_until,
                    grouped_output::finalize_grouped_output,
                },
                value_reducer::finalize_count,
            },
            group::GroupKey,
            pipeline::{
                contracts::{GroupedCursorPage, GroupedRouteStage},
                runtime::GroupedStreamStage,
            },
            route::GroupedExecutionMode,
        },
        query::plan::GroupFieldRef,
    },
    error::InternalError,
    value::Value,
};

pub(in crate::db::executor) use metadata::try_execute_grouped_count_metadata;

pub(in crate::db::executor::aggregate::runtime::grouped_fold) use ingest::materialize_group_key_from_row_view;

// Execute grouped `COUNT(*)` through a dedicated fold path that keeps only one
// canonical grouped-count map instead of the generic grouped reducer stack.
pub(super) fn execute_single_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedCursorPage, InternalError> {
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

    let consistency = route.consistency();
    let key_path = {
        let (_, preparation, _) = stream.fold_inputs_mut();
        GroupedCountKeyPath::for_route(route, preparation.effective_runtime_filter_program())
    };
    let mut grouped_counts = GroupedCountState::new();
    // Empty global input still owns one real, budgeted result group.
    if route.group_fields().is_empty() {
        let group_key = GroupKey::from_group_values(Vec::new())?;
        grouped_counts.insert_new_group(
            group_key.hash(),
            group_key,
            0,
            grouped_execution_context,
        )?;
    }

    let filtered_rows = match key_path {
        GroupedCountKeyPath::DirectSingleField { group_field_index } => {
            fold_single_field_count_rows(consistency, stream, group_field_index, |value| {
                grouped_counts.increment_single_group_value(value, grouped_execution_context)?;
                Ok(false)
            })?
            .0
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Borrowed,
        } => {
            let Some(fields) = route.group_fields().as_direct() else {
                return Err(InternalError::query_executor_invariant());
            };
            fold_filtered_source_rows(consistency, stream, |_, row| {
                grouped_counts.increment_row_borrowed_group_probe(
                    row,
                    fields,
                    grouped_execution_context,
                )
            })?
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Owned,
        } => {
            let Some(fields) = route.group_fields().as_direct() else {
                return Err(InternalError::query_executor_invariant());
            };
            fold_filtered_source_rows(consistency, stream, |_, row| {
                grouped_counts.increment_row_owned_group_key(row, fields, grouped_execution_context)
            })?
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::PathAware,
        } => {
            let Some(fields) = route.group_fields().as_path_aware() else {
                return Err(InternalError::query_executor_invariant());
            };
            fold_filtered_source_rows(consistency, stream, |_, row| {
                grouped_counts.increment_row_path_group_probe(
                    row,
                    fields,
                    grouped_execution_context,
                )
            })?
        }
    };

    // Phase 2: page and project the finalized grouped-count rows directly so
    // this dedicated path does not round-trip through the generic candidate
    // row envelope only to rebuild grouped rows immediately afterwards.
    let (page_rows, next_cursor) =
        finalize_grouped_count_page(route, grouped_projection_spec, grouped_counts.into_groups())?;

    Ok(finalize_grouped_output(
        crate::db::executor::pipeline::contracts::GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        route.group_fields().len(),
    ))
}

// Execute dedicated grouped COUNT(*) through the shared ordered transition
// owner while retaining the direct single-field key extraction hot path.
fn execute_ordered_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedCursorPage, InternalError> {
    let mut transitions = OrderedGroupFoldState::<u32>::new(1);
    if route.group_fields().is_empty() {
        transitions
            .open_group(
                GroupKey::from_group_values(Vec::new())?,
                || 0,
                grouped_execution_context,
            )
            .map_err(GroupError::into_internal_error)?;
    }
    let mut selection = OrderedGroupedPageSelection::new(route, grouped_projection_spec, 1)?;
    let consistency = route.consistency();
    let key_path = {
        let (_, preparation, _) = stream.fold_inputs_mut();
        GroupedCountKeyPath::for_route(route, preparation.effective_runtime_filter_program())
    };
    let (filtered_rows, early_scan_stop) = match key_path {
        GroupedCountKeyPath::DirectSingleField { group_field_index } => {
            fold_single_field_count_rows(consistency, stream, group_field_index, |value| {
                apply_ordered_count_row(
                    &mut transitions,
                    grouped_execution_context,
                    route,
                    || std::iter::once(Ok(&value)),
                    &mut selection,
                )
            })?
        }
        GroupedCountKeyPath::RowView { .. } => {
            fold_filtered_source_rows_until(consistency, stream, |_, row| {
                apply_ordered_count_row(
                    &mut transitions,
                    grouped_execution_context,
                    route,
                    || {
                        route.group_fields().iter().map(|field| match field {
                            GroupFieldRef::Direct(field) => row.require_slot_value(field.index()),
                            GroupFieldRef::PathAware(field) => {
                                resolve_group_field_value(row, field)
                            }
                        })
                    },
                    &mut selection,
                )
            })?
        }
    };

    if !early_scan_stop {
        transitions
            .finish(grouped_execution_context, |group_key, count| {
                selection.push_finalized_values(group_key, vec![finalize_count(u64::from(count))])
            })
            .map_err(GroupError::into_internal_error)?;
    }
    let (page_rows, next_cursor) = selection.finish(route)?;

    Ok(finalize_grouped_output(
        GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        route.group_fields().len(),
    ))
}

// Fold one dedicated count row through the shared ordered transition state
// and canonical incremental grouped-page selector.
fn apply_ordered_count_row<'row, Values>(
    transitions: &mut OrderedGroupFoldState<u32>,
    grouped_execution_context: &mut ExecutionContext,
    route: &GroupedRouteStage,
    group_values: impl Fn() -> Values,
    selection: &mut OrderedGroupedPageSelection<'_>,
) -> Result<bool, InternalError>
where
    Values: Iterator<Item = Result<&'row Value, InternalError>>,
{
    transitions
        .apply_row(
            grouped_execution_context,
            group_values,
            route.direction(),
            || 0,
            |count, _context| {
                *count = count.saturating_add(1);
                Ok(())
            },
            |closed_key, count| {
                selection.push_finalized_values(closed_key, vec![finalize_count(u64::from(count))])
            },
        )
        .map_err(GroupError::into_internal_error)
}
