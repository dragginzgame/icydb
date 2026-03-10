//! Module: db::executor::load::grouped_fold::page_finalize
//! Responsibility: module-local ownership and contracts for db::executor::load::grouped_fold::page_finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        GroupedRow,
        executor::load::{
            GroupedPaginationWindow, GroupedRouteStageProjection, LoadExecutor, PageCursor,
            invariant,
        },
        query::plan::expr::ProjectionSpec,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Apply grouped offset/limit over candidate rows and build grouped continuation output.
    pub(super) fn finalize_grouped_page<R>(
        route: &R,
        grouped_projection_spec: &ProjectionSpec,
        grouped_candidate_rows: Vec<(Value, Vec<Value>)>,
        pagination_window: &GroupedPaginationWindow,
    ) -> Result<(Vec<GroupedRow>, Option<PageCursor>), InternalError>
    where
        R: GroupedRouteStageProjection<E>,
    {
        let limit = pagination_window.limit();
        let initial_offset_for_page = pagination_window.initial_offset_for_page();
        let mut page_rows = Vec::<GroupedRow>::new();
        let mut last_emitted_group_key: Option<Vec<Value>> = None;
        let mut has_more = false;
        let mut groups_skipped_for_offset = 0usize;

        for (group_key_value, aggregate_values) in grouped_candidate_rows {
            if groups_skipped_for_offset < initial_offset_for_page {
                groups_skipped_for_offset = groups_skipped_for_offset.saturating_add(1);
                continue;
            }
            if let Some(limit) = limit
                && page_rows.len() >= limit
            {
                has_more = true;
                break;
            }

            let emitted_group_key = match group_key_value {
                Value::List(values) => values,
                value => {
                    return Err(invariant(format!(
                        "grouped canonical key must be Value::List, found {value:?}"
                    )));
                }
            };
            last_emitted_group_key = Some(emitted_group_key.clone());
            let projected_row = Self::project_grouped_row_from_projection(
                grouped_projection_spec,
                route.projection_layout(),
                route.group_fields(),
                route.grouped_aggregate_exprs(),
                emitted_group_key.as_slice(),
                aggregate_values.as_slice(),
            )?;
            page_rows.push(projected_row);
            debug_assert!(
                limit.is_none_or(|bounded_limit| page_rows.len() <= bounded_limit),
                "grouped page rows must not exceed explicit page limit",
            );
        }

        let next_cursor = if has_more {
            last_emitted_group_key.map(|last_group_key| route.grouped_next_cursor(last_group_key))
        } else {
            None
        }
        .transpose()?;

        Ok((page_rows, next_cursor))
    }
}
