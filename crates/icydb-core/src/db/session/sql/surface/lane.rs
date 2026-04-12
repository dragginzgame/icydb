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
    QueryFrom,
    ExecuteSql,
    ExecuteSqlGrouped,
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
            "query_from_sql rejects EXPLAIN; parse SQL first and use the dedicated EXPLAIN helpers"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Describe) => {
            "query_from_sql rejects DESCRIBE; use describe_entity(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowIndexes) => {
            "query_from_sql rejects SHOW INDEXES; use show_indexes(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowColumns) => {
            "query_from_sql rejects SHOW COLUMNS; use show_columns(...)"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::ShowEntities) => {
            "query_from_sql rejects SHOW ENTITIES; use show_entities()"
        }
        (SqlSurface::QueryFrom, SqlLaneKind::Query) => {
            "query_from_sql accepts SELECT or DELETE only"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::Explain) => {
            "execute_sql rejects EXPLAIN; parse SQL first and use the dedicated EXPLAIN helpers"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::Describe) => {
            "execute_sql rejects DESCRIBE; use describe_entity(...)"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::ShowIndexes) => {
            "execute_sql rejects SHOW INDEXES; use show_indexes(...)"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::ShowColumns) => {
            "execute_sql rejects SHOW COLUMNS; use show_columns(...)"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::ShowEntities) => {
            "execute_sql rejects SHOW ENTITIES; use show_entities()"
        }
        (SqlSurface::ExecuteSql, SqlLaneKind::Query) => "execute_sql accepts SELECT or DELETE only",
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::Explain) => {
            "execute_sql_grouped rejects EXPLAIN"
        }
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::Describe) => {
            "execute_sql_grouped rejects DESCRIBE"
        }
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::ShowIndexes) => {
            "execute_sql_grouped rejects SHOW INDEXES"
        }
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::ShowColumns) => {
            "execute_sql_grouped rejects SHOW COLUMNS"
        }
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::ShowEntities) => {
            "execute_sql_grouped rejects SHOW ENTITIES"
        }
        (SqlSurface::ExecuteSqlGrouped, SqlLaneKind::Query) => {
            "execute_sql_grouped requires grouped SELECT"
        }
        (SqlSurface::Explain, SqlLaneKind::Describe) => "explain_sql rejects DESCRIBE",
        (SqlSurface::Explain, SqlLaneKind::ShowIndexes) => "explain_sql rejects SHOW INDEXES",
        (SqlSurface::Explain, SqlLaneKind::ShowColumns) => "explain_sql rejects SHOW COLUMNS",
        (SqlSurface::Explain, SqlLaneKind::ShowEntities) => "explain_sql rejects SHOW ENTITIES",
        (SqlSurface::Explain, SqlLaneKind::Query | SqlLaneKind::Explain) => {
            "explain_sql requires EXPLAIN"
        }
    }
}
