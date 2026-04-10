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
pub(in crate::db::executor) use materialize::mark_projection_referenced_slots;
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
#[cfg(all(feature = "sql", any(test, feature = "structural-read-metrics")))]
pub(in crate::db::executor) use materialize::record_sql_projection_full_row_decode_materialization;
pub(in crate::db::executor) use materialize::{
    PreparedProjectionShape, PreparedSlotProjectionValidation, prepare_projection_shape,
    validate_prepared_projection_over_slot_rows,
};
#[cfg(all(feature = "sql", feature = "structural-read-metrics"))]
pub use materialize::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};
#[cfg(feature = "sql")]
pub(in crate::db) use materialize::{
    execute_sql_projection_rows_for_canister, execute_sql_projection_text_rows_for_canister,
};
