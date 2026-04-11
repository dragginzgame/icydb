//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod eval;
mod grouped;
mod materialize;
#[cfg(test)]
mod tests;

pub(in crate::db::executor) use eval::*;
pub(in crate::db::executor) use grouped::*;
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub use materialize::SqlProjectionTextExecutorAttribution;
#[cfg(all(feature = "sql", feature = "perf-attribution"))]
pub(in crate::db) use materialize::attribute_sql_projection_text_rows_for_canister;
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
#[cfg(all(feature = "sql", any(test, feature = "structural-read-metrics")))]
pub(in crate::db::executor) use materialize::record_sql_projection_full_row_decode_materialization;
pub(in crate::db::executor) use materialize::{
    PreparedProjectionShape, PreparedSlotProjectionValidation, prepare_projection_shape_from_plan,
    validate_prepared_projection_row,
};
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
pub use materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "sql")]
pub(in crate::db) use materialize::{
    execute_sql_projection_rows_for_canister, execute_sql_projection_text_rows_for_canister,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use materialize::{
    project_sql_distinct_projection_slot_rows_for_dispatch,
    project_sql_projection_slot_rows_for_dispatch,
    render_sql_distinct_projection_slot_rows_for_dispatch,
    render_sql_projection_slot_rows_for_dispatch,
};
