//! Module: db::session::sql::projection::runtime::covering
//! Responsibility: session-owned covering SQL projection execution over
//! planner-proven index routes.
//! Does not own: generic structural page materialization or SQL text rendering.
//! Boundary: pure and hybrid covering paths stay below the runtime root while
//! sharing one local row-assembly seam.

mod hybrid;
mod pure;
mod shared;

pub(in crate::db::session::sql::projection::runtime) use crate::db::session::sql::projection::runtime::covering::{
    hybrid::try_execute_hybrid_covering_sql_projection_rows_for_canister,
    pure::try_execute_covering_sql_projection_rows_for_canister,
};
