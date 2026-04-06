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
#[cfg(feature = "sql")]
pub(in crate::db::executor) use materialize::direct_projection_field_slots;
#[cfg(feature = "sql")]
pub(in crate::db) use materialize::execute_sql_projection_rows_for_canister;
#[cfg(all(feature = "sql", test))]
pub(in crate::db::executor::projection) use materialize::project_rows_from_projection;
pub(in crate::db::executor) use materialize::{
    evaluate_grouped_projection_values, validate_projection_over_slot_rows,
};
