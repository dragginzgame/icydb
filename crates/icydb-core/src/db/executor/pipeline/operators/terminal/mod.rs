//! Module: executor::pipeline::operators::terminal
//! Responsibility: terminal load row-collector materialization seam.
//! Does not own: aggregate fold reducers or access-path planning/routing policy.
//! Boundary: owns cursorless load row-collector short-path execution mechanics.

mod runtime;

#[cfg(feature = "sql")]
pub(in crate::db::executor) use runtime::{
    PreparedSqlExecutionProjection, prepare_sql_execution_projection,
};
