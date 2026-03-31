//! Module: db::session::sql::surface::lane
//! Responsibility: module-local ownership and contracts for db::session::sql::surface::lane.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::sql::lowering::{LoweredSqlCommand, LoweredSqlLaneKind, lowered_sql_command_lane};

// Canonical reduced SQL lane kind used by session entrypoint gate checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlLaneKind {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
}

// Session SQL surfaces that enforce explicit wrong-lane fail-closed contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlSurface {
    QueryFrom,
    Explain,
}

// Resolve one generic-free lowered SQL command to the session lane taxonomy.
pub(in crate::db::session::sql) const fn session_sql_lane(
    command: &LoweredSqlCommand,
) -> SqlLaneKind {
    match lowered_sql_command_lane(command) {
        LoweredSqlLaneKind::Query => SqlLaneKind::Query,
        LoweredSqlLaneKind::Explain => SqlLaneKind::Explain,
        LoweredSqlLaneKind::Describe => SqlLaneKind::Describe,
        LoweredSqlLaneKind::ShowIndexes => SqlLaneKind::ShowIndexes,
        LoweredSqlLaneKind::ShowColumns => SqlLaneKind::ShowColumns,
        LoweredSqlLaneKind::ShowEntities => SqlLaneKind::ShowEntities,
    }
}

// Render one deterministic unsupported-lane message for one SQL surface.
pub(in crate::db::session::sql) const fn unsupported_sql_lane_message(
    surface: SqlSurface,
    lane: SqlLaneKind,
) -> &'static str {
    match (surface, lane) {
        (SqlSurface::QueryFrom, SqlLaneKind::Explain) => {
            "query_from_sql rejects EXPLAIN; use execute_sql_dispatch"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Describe) => {
            "query_from_sql rejects DESCRIBE; use execute_sql_dispatch"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowIndexes) => {
            "query_from_sql rejects SHOW INDEXES; use execute_sql_dispatch"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowColumns) => {
            "query_from_sql rejects SHOW COLUMNS; use execute_sql_dispatch"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowEntities) => {
            "query_from_sql rejects SHOW ENTITIES; use execute_sql_dispatch"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Query) => {
            "query_from_sql accepts SELECT or DELETE only"
        }
        (SqlSurface::Explain, SqlLaneKind::Describe) => {
            "explain_sql rejects DESCRIBE; use execute_sql_dispatch"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowIndexes) => {
            "explain_sql rejects SHOW INDEXES; use execute_sql_dispatch"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowColumns) => {
            "explain_sql rejects SHOW COLUMNS; use execute_sql_dispatch"
        }
        (SqlSurface::Explain, SqlLaneKind::ShowEntities) => {
            "explain_sql rejects SHOW ENTITIES; use execute_sql_dispatch"
        }
        (SqlSurface::Explain, SqlLaneKind::Query | SqlLaneKind::Explain) => {
            "explain_sql requires EXPLAIN"
        }
    }
}
