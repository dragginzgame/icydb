use crate::{
    db::{
        executor::{
            aggregate::ExecutionContext,
            load::{
                GroupedCursorPage, GroupedFoldStage, GroupedRouteStage, GroupedStreamStage,
                LoadExecutor,
            },
        },
        query::plan::{expr::ProjectionSpec, grouped_cursor_policy_violation},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute grouped global DISTINCT field-target fold path and emit one folded page when active.
    pub(super) fn try_execute_global_distinct_fold(
        route: &GroupedRouteStage<E>,
        stream: &mut GroupedStreamStage<'_, E>,
        grouped_execution_context: &mut ExecutionContext,
        grouped_projection_spec: &ProjectionSpec,
        scanned_rows: &mut usize,
        filtered_rows: &mut usize,
    ) -> Result<Option<GroupedFoldStage>, InternalError> {
        let Some((aggregate_kind, target_field)) = route.global_distinct_field_aggregate.as_ref()
        else {
            return Ok(None);
        };
        if let Some(grouped_plan) = route.plan.grouped_plan()
            && let Some(violation) =
                grouped_cursor_policy_violation(grouped_plan, !route.cursor.is_empty())
        {
            return Err(crate::db::executor::load::invariant(
                violation.invariant_message(),
            ));
        }
        let compiled_predicate = stream.execution_preparation.compiled_predicate();

        let global_row = Self::execute_global_distinct_field_aggregate(
            &route.plan,
            &stream.ctx,
            &mut stream.resolved,
            compiled_predicate,
            grouped_execution_context,
            (*aggregate_kind, target_field.as_str()),
            (scanned_rows, filtered_rows),
        )?;
        let page_rows = Self::page_global_distinct_grouped_row(
            global_row,
            route.plan.scalar_plan().page.as_ref(),
        );
        let page_rows = Self::project_grouped_rows_from_projection(
            grouped_projection_spec,
            &route.projection_layout,
            route.group_fields.as_slice(),
            route.grouped_aggregate_exprs.as_slice(),
            page_rows,
        )?;
        let rows_scanned = stream
            .resolved
            .rows_scanned_override
            .unwrap_or(*scanned_rows);
        let optimization = stream.resolved.optimization;
        let index_predicate_applied = stream.resolved.index_predicate_applied;
        let index_predicate_keys_rejected = stream.resolved.index_predicate_keys_rejected;
        let distinct_keys_deduped = stream
            .resolved
            .distinct_keys_deduped_counter
            .as_ref()
            .map_or(0, |counter| counter.get());

        Ok(Some(GroupedFoldStage {
            page: GroupedCursorPage {
                rows: page_rows,
                next_cursor: None,
            },
            filtered_rows: *filtered_rows,
            check_filtered_rows_upper_bound: false,
            rows_scanned,
            optimization,
            index_predicate_applied,
            index_predicate_keys_rejected,
            distinct_keys_deduped,
        }))
    }
}
