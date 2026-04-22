//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod eval;
mod grouped;
mod materialize;
#[cfg(test)]
mod tests;

pub(in crate::db) use eval::{
    ProjectionEvalError, ScalarProjectionExpr, eval_builder_expr_for_value_preview,
    eval_canonical_scalar_projection_expr_with_required_value_reader_cow,
    eval_effective_runtime_filter_program_with_value_cow_reader,
    eval_effective_runtime_filter_program_with_value_ref_reader,
};
pub(in crate::db::executor) use eval::{
    eval_scalar_projection_expr_with_value_reader,
    eval_scalar_projection_expr_with_value_ref_reader,
};
pub(in crate::db::executor) use grouped::*;
pub(in crate::db) use grouped::{
    GroupedProjectionExpr, GroupedRowView, compile_grouped_projection_expr,
    eval_grouped_projection_expr, evaluate_grouped_having_expr,
};
#[cfg(test)]
pub(in crate::db) use materialize::PreparedProjectionPlan;
#[cfg(test)]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
pub(in crate::db) use materialize::{
    PreparedProjectionShape, prepare_projection_shape_from_plan,
    visit_prepared_projection_values_with_required_value_reader_cow,
};
pub(in crate::db::executor) use materialize::{
    PreparedSlotProjectionValidation, ProjectionValidationRow, validate_prepared_projection_row,
};
#[cfg(test)]
pub(in crate::db) use tests::projection_eval_data_row_for_materialize_tests;
#[cfg(test)]
pub(in crate::db) use tests::projection_eval_row_layout_for_materialize_tests;
