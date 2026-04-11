//! Module: db::session::sql::surface
//! Responsibility: classify lowered SQL commands onto the supported session SQL
//! surfaces and route families.
//! Does not own: execution, planning, or result shaping once a surface is chosen.
//! Boundary: keeps lane and route classification together above dispatch entrypoints.

mod lane;
mod route;

pub use crate::db::session::sql::surface::route::{
    SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
};

pub(in crate::db::session::sql) use crate::db::session::sql::surface::{
    lane::{SqlLaneKind, SqlSurface, session_sql_lane, unsupported_sql_lane_message},
    route::sql_statement_route_from_statement,
};
