use crate::{
    db::{
        executor::{
            ContinuationEngine,
            load::{GroupedRouteStage, LoadExecutor, PageCursor},
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
    pub(super) fn finalize_grouped_page(
        route: &GroupedRouteStage<E>,
        grouped_projection_spec: &ProjectionSpec,
        grouped_candidate_rows: Vec<(Value, Vec<Value>)>,
        limit: Option<usize>,
        initial_offset_for_page: usize,
        resume_initial_offset: u32,
    ) -> Result<(Vec<crate::db::GroupedRow>, Option<PageCursor>), InternalError> {
        let mut page_rows = Vec::<crate::db::GroupedRow>::new();
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
                    return Err(crate::db::executor::load::invariant(format!(
                        "grouped canonical key must be Value::List, found {value:?}"
                    )));
                }
            };
            last_emitted_group_key = Some(emitted_group_key.clone());
            let projected_row = Self::project_grouped_row_from_projection(
                grouped_projection_spec,
                &route.planner_payload.projection_layout,
                route.planner_payload.group_fields.as_slice(),
                route.planner_payload.grouped_aggregate_exprs.as_slice(),
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
            last_emitted_group_key.map(|last_group_key| {
                PageCursor::Grouped(ContinuationEngine::grouped_next_cursor_token(
                    route.execution_context.continuation_signature,
                    last_group_key,
                    resume_initial_offset,
                ))
            })
        } else {
            None
        };

        Ok((page_rows, next_cursor))
    }
}
