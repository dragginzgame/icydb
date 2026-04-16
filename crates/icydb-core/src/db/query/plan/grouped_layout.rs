//! Module: query::plan::grouped_layout
//! Responsibility: planner-owned grouped projection layout invariant checks.
//! Does not own: grouped execution routing or fold/runtime mechanics.
//! Boundary: validates grouped projection structural contracts before executor handoff.

use crate::{db::query::plan::PlannedProjectionLayout, error::InternalError};

/// Validate grouped projection layout invariants for one grouped handoff shape.
pub(in crate::db) fn validate_grouped_projection_layout(
    projection_layout: &PlannedProjectionLayout,
) -> Result<(), InternalError> {
    let group_positions = projection_layout.group_field_positions();
    let aggregate_positions = projection_layout.aggregate_positions();

    if !group_positions
        .windows(2)
        .all(|window| window[0] < window[1])
    {
        return Err(PlannedProjectionLayout::group_field_positions_not_strictly_increasing());
    }
    if !aggregate_positions
        .windows(2)
        .all(|window| window[0] < window[1])
    {
        return Err(PlannedProjectionLayout::aggregate_positions_not_strictly_increasing());
    }
    if let (Some(last_group_position), Some(first_aggregate_position)) =
        (group_positions.last(), aggregate_positions.first())
        && last_group_position >= first_aggregate_position
    {
        return Err(PlannedProjectionLayout::group_fields_must_precede_aggregates());
    }

    Ok(())
}
