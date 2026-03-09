//! Module: query::plan::validate::grouped
//! Responsibility: grouped-plan semantic validation slices (policy, cursor, structure, projection).
//! Does not own: executor runtime fail-closed checks or grouped execution orchestration.
//! Boundary: planner validation composes these helpers before route/executor handoff.

mod cursor;
mod policy;
mod projection_expr;
mod structure;

pub(in crate::db::query::plan::validate) use cursor::validate_group_cursor_constraints;
pub(in crate::db::query::plan::validate) use policy::validate_group_policy;
#[cfg(test)]
pub(in crate::db::query) use projection_expr::validate_group_projection_expr_compatibility_for_test;
pub(in crate::db::query::plan::validate) use projection_expr::validate_projection_expr_types;
pub(in crate::db::query::plan::validate) use structure::validate_group_structure;
