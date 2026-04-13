//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod eval;
mod grouped;
mod materialize;
#[cfg(test)]
mod tests;

pub(in crate::db::executor) use eval::eval_scalar_projection_expr_with_value_reader;
pub(in crate::db) use eval::{
    ProjectionEvalError, ScalarProjectionExpr, eval_binary_expr,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    eval_text_projection_expr_with_value,
};
pub(in crate::db::executor) use grouped::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(in crate::db) use materialize::PreparedProjectionPlan;
#[cfg(test)]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
pub(in crate::db) use materialize::{
    PreparedProjectionShape, prepare_projection_shape_from_plan,
    visit_prepared_projection_values_with_required_value_reader_cow,
};
pub(in crate::db::executor) use materialize::{
    PreparedSlotProjectionValidation, validate_prepared_projection_row,
};
#[cfg(test)]
pub(in crate::db) use tests::projection_eval_row_layout_for_materialize_tests;
