//! Module: executor::aggregate::runtime::grouped_fold::utils::boundary
//! Responsibility: grouped pagination boundary ordering helpers.
//! Boundary: owns direction-aware canonical grouped-key boundary comparison.

use std::cmp::Ordering;

use crate::{
    db::{direction::Direction, numeric::canonical_value_compare},
    value::Value,
};

// Compare grouped boundary values in the active grouped execution direction.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn compare_grouped_boundary_values(
    direction: Direction,
    left: &Value,
    right: &Value,
) -> Ordering {
    match direction {
        Direction::Asc => canonical_value_compare(left, right),
        Direction::Desc => canonical_value_compare(right, left),
    }
}

// Return true when one candidate remains beyond the grouped continuation
// boundary in the active grouped execution direction.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn grouped_resume_boundary_allows_candidate(
    direction: Direction,
    candidate_key: &Value,
    resume_boundary: &Value,
) -> bool {
    compare_grouped_boundary_values(direction, candidate_key, resume_boundary).is_gt()
}
