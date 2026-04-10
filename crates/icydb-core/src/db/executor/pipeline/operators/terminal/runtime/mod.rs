//! Module: db::executor::pipeline::operators::terminal::runtime
//! Responsibility: terminal-runtime boundary for cursorless load row collection and SQL-specific short-path materialization.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes the terminal runtime surface while keeping the current SQL-heavy implementation in owner-local children.

mod sql;

#[cfg(feature = "sql")]
pub(in crate::db::executor) use crate::db::executor::pipeline::operators::terminal::runtime::sql::{
    PreparedSqlExecutionProjection, prepare_sql_execution_projection,
};
