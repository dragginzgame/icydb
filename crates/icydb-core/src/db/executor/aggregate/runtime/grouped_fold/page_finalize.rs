//! Module: db::executor::aggregate::runtime::grouped_fold::page_finalize
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::page_finalize.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        GroupedRow,
        executor::{
            GroupedPaginationWindow,
            aggregate::runtime::grouped_output::project_grouped_rows_from_projection,
            pipeline::contracts::{GroupedRouteStage, PageCursor},
        },
        query::plan::expr::ProjectionSpec,
    },
    error::InternalError,
    value::Value,
};

// Apply grouped offset/limit over candidate rows and build grouped continuation output.
pub(super) fn finalize_grouped_page(
    route: &GroupedRouteStage,
    grouped_projection_spec: &ProjectionSpec,
    grouped_candidate_rows: Vec<(Value, Vec<Value>)>,
    pagination_window: &GroupedPaginationWindow,
) -> Result<(Vec<GroupedRow>, Option<PageCursor>), InternalError> {
    let (page_rows, next_cursor_boundary) =
        finalize_grouped_page_rows(grouped_candidate_rows, pagination_window)?;
    let page_rows = project_grouped_rows_from_projection(
        grouped_projection_spec,
        route.projection_layout(),
        route.group_fields(),
        route.grouped_aggregate_execution_specs(),
        page_rows,
    )?;
    let next_cursor = next_cursor_boundary
        .map(|last_group_key| route.grouped_next_cursor(last_group_key))
        .transpose()?;

    Ok((page_rows, next_cursor))
}

// Apply grouped offset/limit and projection over candidate rows. Returns one
// optional last-emitted grouped boundary key when pagination indicates has-more.
fn finalize_grouped_page_rows(
    grouped_candidate_rows: Vec<(Value, Vec<Value>)>,
    pagination_window: &GroupedPaginationWindow,
) -> Result<(Vec<GroupedRow>, Option<Vec<Value>>), InternalError> {
    let limit = pagination_window.limit();
    let initial_offset_for_page = pagination_window.initial_offset_for_page();
    let mut page_rows = Vec::<GroupedRow>::new();
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
                return Err(GroupedRouteStage::canonical_group_key_must_be_list(&value));
            }
        };
        page_rows.push(GroupedRow::new(emitted_group_key, aggregate_values));
        debug_assert!(
            limit.is_none_or(|bounded_limit| page_rows.len() <= bounded_limit),
            "grouped page rows must not exceed explicit page limit",
        );
    }

    // Only clone the final emitted key when pagination actually needs a cursor.
    // The previous shape cloned every emitted grouped key even when only the
    // last page boundary was observable.
    let next_cursor_boundary = if has_more {
        page_rows.last().map(|row| row.group_key().to_vec())
    } else {
        None
    };

    Ok((page_rows, next_cursor_boundary))
}
