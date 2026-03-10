//! Module: query::plan::grouped_layout
//! Responsibility: planner-owned grouped projection layout invariant checks.
//! Does not own: grouped execution routing or fold/runtime mechanics.
//! Boundary: validates grouped projection structural contracts before executor handoff.

use crate::{db::query::plan::PlannedProjectionLayout, error::InternalError};

/// Validate grouped projection layout invariants for one grouped handoff shape.
pub(in crate::db) fn validate_grouped_projection_layout(
    projection_layout: &PlannedProjectionLayout,
    group_fields_len: usize,
    aggregate_exprs_len: usize,
) -> Result<(), InternalError> {
    let group_positions = projection_layout.group_field_positions();
    let aggregate_positions = projection_layout.aggregate_positions();
    if group_positions.len() != group_fields_len {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(format!(
                "grouped projection layout group-field count mismatch: layout={}, handoff={group_fields_len}",
                group_positions.len()
            )),
        ));
    }
    if aggregate_positions.len() != aggregate_exprs_len {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(format!(
                "grouped projection layout aggregate count mismatch: layout={}, handoff={aggregate_exprs_len}",
                aggregate_positions.len()
            )),
        ));
    }

    if !group_positions
        .windows(2)
        .all(|window| window[0] < window[1])
    {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(
                "grouped projection layout group-field positions must be strictly increasing",
            ),
        ));
    }
    if !aggregate_positions
        .windows(2)
        .all(|window| window[0] < window[1])
    {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(
                "grouped projection layout aggregate positions must be strictly increasing",
            ),
        ));
    }
    if let (Some(last_group_position), Some(first_aggregate_position)) =
        (group_positions.last(), aggregate_positions.first())
        && last_group_position >= first_aggregate_position
    {
        return Err(InternalError::planner_invariant(
            InternalError::executor_invariant_message(
                "grouped projection layout must keep group fields before aggregate terminals",
            ),
        ));
    }

    Ok(())
}
