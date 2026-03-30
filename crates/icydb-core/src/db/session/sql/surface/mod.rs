//! Module: db::session::sql::surface
//! Responsibility: module-local ownership and contracts for db::session::sql::surface.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod lane;
mod route;

pub use crate::db::session::sql::surface::route::{
    SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
};

pub(in crate::db::session::sql) use crate::db::session::sql::surface::{
    lane::{SqlLaneKind, SqlSurface, session_sql_lane, unsupported_sql_lane_message},
    route::sql_statement_route_from_statement,
};
