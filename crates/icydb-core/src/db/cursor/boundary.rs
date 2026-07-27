//! Module: cursor::boundary
//! Responsibility: cursor boundary slot modeling and deterministic cursor boundary handling.
//! Does not own: planner query validation policy or access-path execution routing.
//! Boundary: defines cursor-boundary domain types shared by cursor planning/runtime paths.

use crate::{
    db::{cursor::CursorPlanError, query::plan::OrderDirection},
    value::Value,
};
use serde::Deserialize;
use std::cmp::Ordering;

///
/// CursorBoundarySlot
/// Slot value used for deterministic cursor boundaries.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) enum CursorBoundarySlot {
    Missing,
    Present(Value),
}

///
/// CursorBoundary
/// Ordered boundary tuple for continuation pagination.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(in crate::db) struct CursorBoundary {
    pub(in crate::db) slots: Vec<CursorBoundarySlot>,
}

/// Apply one order direction to one base slot ordering.
#[must_use]
pub(in crate::db) const fn apply_order_direction(
    ordering: Ordering,
    direction: OrderDirection,
) -> Ordering {
    match direction {
        OrderDirection::Asc => ordering,
        OrderDirection::Desc => ordering.reverse(),
    }
}

/// Validate continuation initial-offset compatibility.
pub(in crate::db::cursor) const fn validate_cursor_window_offset(
    expected_initial_offset: u32,
    actual_initial_offset: u32,
) -> Result<(), CursorPlanError> {
    if actual_initial_offset != expected_initial_offset {
        return Err(CursorPlanError::continuation_cursor_window_mismatch(
            expected_initial_offset,
            actual_initial_offset,
        ));
    }

    Ok(())
}

// Resolve one plain order field type from canonical schema info.
