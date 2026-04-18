//! Module: query::plan::validate::grouped
//! Responsibility: grouped-plan semantic validation slices (policy, cursor, structure, projection).
//! Does not own: executor runtime fail-closed checks or grouped execution orchestration.
//! Boundary: planner validation composes these helpers before route/executor handoff.

mod cursor;
mod policy;
mod projection_expr;
mod structure;

pub(crate) use cursor::validate_group_cursor_constraints;
pub(crate) use policy::validate_group_policy;
#[cfg(test)]
pub(crate) use projection_expr::validate_group_projection_expr_compatibility;
pub(crate) use projection_expr::validate_projection_expr_types;
pub(crate) use structure::validate_group_structure;
