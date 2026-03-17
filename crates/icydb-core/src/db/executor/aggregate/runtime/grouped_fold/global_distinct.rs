//! Module: db::executor::aggregate::runtime::grouped_fold::global_distinct
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::global_distinct.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{
            aggregate::{
                ExecutionContext, runtime::grouped_distinct::GlobalDistinctFieldAggregateKind,
                runtime::grouped_output::project_grouped_rows_from_projection,
            },
            pipeline::contracts::{
                GroupedCursorPage, GroupedFoldStage, GroupedRouteStageProjection,
                GroupedStreamStage, LoadExecutor,
            },
        },
        query::plan::{GroupedDistinctExecutionStrategy, expr::ProjectionSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute grouped global DISTINCT field-target fold path and emit one folded page when active.
    pub(super) fn try_execute_global_distinct_fold<R>(
        route: &R,
        stream: &mut GroupedStreamStage<'_, E>,
        grouped_execution_context: &mut ExecutionContext,
        grouped_projection_spec: &ProjectionSpec,
        scanned_rows: &mut usize,
        filtered_rows: &mut usize,
    ) -> Result<Option<GroupedFoldStage>, InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        let (aggregate_kind, target_field) = match route.grouped_distinct_execution_strategy() {
            GroupedDistinctExecutionStrategy::None => return Ok(None),
            GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount { target_field } => (
                GlobalDistinctFieldAggregateKind::Count,
                target_field.as_str(),
            ),
            GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum { target_field } => {
                (GlobalDistinctFieldAggregateKind::Sum, target_field.as_str())
            }
            GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg { target_field } => {
                (GlobalDistinctFieldAggregateKind::Avg, target_field.as_str())
            }
        };
        let (ctx, execution_preparation, resolved) = stream.parts_mut();
        let compiled_predicate = execution_preparation.compiled_predicate();

        let global_row = Self::execute_global_distinct_field_aggregate(
            route.consistency(),
            ctx,
            resolved,
            compiled_predicate,
            grouped_execution_context,
            (target_field, aggregate_kind),
            (scanned_rows, filtered_rows),
        )?;
        let grouped_window = route.grouped_pagination_window();
        let page_rows = Self::page_global_distinct_grouped_row(
            global_row,
            grouped_window.initial_offset_for_page(),
            grouped_window.limit(),
        );
        let page_rows = project_grouped_rows_from_projection(
            grouped_projection_spec,
            route.projection_layout(),
            route.group_fields(),
            route.grouped_aggregate_exprs(),
            page_rows,
        )?;
        Ok(Some(GroupedFoldStage::from_grouped_stream(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor: None,
            },
            *filtered_rows,
            false,
            stream,
            *scanned_rows,
        )))
    }
}
