//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod covering;
mod eval;
mod facade;
mod grouped;
mod materialize;
mod path;
pub(in crate::db) use covering::CoveringProjectionMetricsRecorder;
pub(in crate::db::executor) use covering::{
    PreparedCoveringProjectionRuntime, try_execute_prepared_covering_projection_rows_for_canister,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use covering::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
pub(in crate::db) use eval::ProjectionEvalError;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use eval::eval_compiled_expr_with_value_ref_reader;
#[cfg(feature = "sql")]
pub(in crate::db) use eval::eval_compiled_filter_expr_with_required_slot_reader;
pub(in crate::db::executor) use eval::{
    eval_compiled_expr_with_value_reader, eval_effective_runtime_filter_program_with_slot_reader,
    eval_effective_runtime_filter_program_with_value_cow_reader,
};
#[cfg(feature = "sql")]
pub(in crate::db) use facade::StructuralProjectionScanBudget;
#[cfg(feature = "sql")]
pub(in crate::db) use facade::execute_structural_projection_rows;
pub(in crate::db) use facade::{
    StructuralProjectionExecutionRoute, StructuralProjectionRequest,
    execute_structural_projection_page, execute_structural_projection_page_with_route,
};
pub(in crate::db::executor) use grouped::*;
pub(in crate::db::executor) use grouped::{
    GroupedRowView, compile_grouped_projection_expr, evaluate_grouped_having_expr,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use materialize::DistinctProjectionMetricsRecorder;
pub(in crate::db) use materialize::MaterializedProjectionRows;
pub(in crate::db) use materialize::ProjectionMaterializationMetricsRecorder;
pub(in crate::db) use materialize::project;
pub(in crate::db::executor::projection) use materialize::{
    DistinctProjectionRuntime, ProjectionDistinctStrategy, ProjectionDistinctWindow,
    projection_distinct_strategy,
};
pub(in crate::db) use materialize::{
    PreparedProjectionContract, prepare_projection_contract_from_plan,
};
pub(in crate::db::executor) use materialize::{
    ProjectionValidationRow, validate_prepared_projection_row,
};
pub(in crate::db::executor::projection) use materialize::{
    project_admitted_page, project_distinct,
};
pub(in crate::db::executor) use path::resolve_path_segments;
pub(in crate::db::executor) use path::resolve_value_field_path;
