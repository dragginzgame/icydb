//! Module: db::session::sql::execute::write_returning
//! Responsibility: SQL write `RETURNING` and statement-result shaping.
//! Does not own: SQL write selection, mutation execution, or patch construction.
//! Boundary: converts already-mutated rows into the public SQL statement result shape.

mod bounds;
mod projection;

pub(in crate::db::session::sql::execute) use bounds::validate_sql_materialized_returning_bounds;
pub(in crate::db::session::sql) use projection::SqlReturningProjectionPlan;
pub(in crate::db::session::sql::execute) use projection::sql_returning_statement_projection;
