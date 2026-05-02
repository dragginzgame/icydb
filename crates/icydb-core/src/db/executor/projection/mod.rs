//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod covering;
mod eval;
#[cfg(feature = "sql")]
mod facade;
mod grouped;
mod materialize;
mod path;
#[cfg(test)]
mod tests;

#[cfg(feature = "sql")]
pub(in crate::db) use covering::CoveringProjectionMetricsRecorder;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use covering::try_execute_covering_projection_rows_for_canister;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use covering::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
pub(in crate::db) use eval::ProjectionEvalError;
pub(in crate::db::executor) use eval::{
    eval_compiled_expr_with_value_reader, eval_compiled_expr_with_value_ref_reader,
    eval_effective_runtime_filter_program_with_slot_reader,
    eval_effective_runtime_filter_program_with_value_cow_reader,
    eval_effective_runtime_filter_program_with_value_ref_reader,
};
#[cfg(feature = "sql")]
pub(in crate::db) use facade::{StructuralProjectionRequest, execute_structural_projection_result};
pub(in crate::db::executor) use grouped::*;
pub(in crate::db::executor) use grouped::{
    GroupedRowView, compile_grouped_projection_expr, evaluate_grouped_having_expr,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use materialize::MaterializedProjectionRows;
#[cfg(test)]
pub(in crate::db) use materialize::PreparedProjectionPlan;
#[cfg(feature = "sql")]
pub(in crate::db::executor::projection) use materialize::ProjectionDistinctWindow;
#[cfg(test)]
pub(in crate::db) use materialize::project;
#[cfg(all(feature = "sql", not(test)))]
pub(in crate::db::executor) use materialize::project;
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use materialize::project_distinct;
#[cfg(all(feature = "sql", not(test)))]
pub(in crate::db::executor::projection) use materialize::project_distinct;
#[cfg(test)]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
pub(in crate::db) use materialize::{
    PreparedProjectionShape, ProjectionMaterializationMetricsRecorder,
    prepare_projection_shape_from_plan,
};
pub(in crate::db::executor) use materialize::{
    PreparedSlotProjectionValidation, ProjectionValidationRow, validate_prepared_projection_row,
};
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use materialize::{
    count_borrowed_data_row_views_for_test, count_borrowed_identity_data_row_views_for_test,
    count_borrowed_slot_row_views_for_test,
};
#[cfg(test)]
pub(in crate::db) use tests::projection_eval_data_row_for_materialize_tests;
#[cfg(test)]
pub(in crate::db) use tests::projection_eval_row_layout_for_materialize_tests;
