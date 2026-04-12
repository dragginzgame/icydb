//! Module: db::session::sql::surface::lane
//! Responsibility: classify lowered SQL commands into the canonical session
//! lane kinds used by statement and explain entrypoint guards.
//! Does not own: detailed route classification or command execution.
//! Boundary: provides the narrow lane taxonomy for session SQL gate checks.

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
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::session::sql) const fn unsupported_sql_lane_message(
    surface: SqlSurface,
    lane: SqlLaneKind,
) -> &'static str {
    match (surface, lane) {
        (SqlSurface::Explain, SqlLaneKind::Describe) => "explain_sql rejects DESCRIBE",
        (SqlSurface::Explain, SqlLaneKind::ShowIndexes) => "explain_sql rejects SHOW INDEXES",
        (SqlSurface::Explain, SqlLaneKind::ShowColumns) => "explain_sql rejects SHOW COLUMNS",
        (SqlSurface::Explain, SqlLaneKind::ShowEntities) => "explain_sql rejects SHOW ENTITIES",
        (SqlSurface::Explain, SqlLaneKind::Query | SqlLaneKind::Explain) => {
            "explain_sql requires EXPLAIN"
        }
    }
}
