//! Module: executor::projection
//! Responsibility: scalar projection expression evaluation over materialized rows.
//! Does not own: planner expression typing/validation or grouped aggregate folds.
//! Boundary: pure evaluator + projected-row materialization for scalar load paths.

mod eval;
mod grouped;
mod materialize;
#[cfg(test)]
mod tests;

#[cfg_attr(not(test), expect(unused_imports))]
pub(in crate::db::executor) use eval::*;
pub(in crate::db::executor) use grouped::*;
pub(in crate::db::executor) use materialize::*;
