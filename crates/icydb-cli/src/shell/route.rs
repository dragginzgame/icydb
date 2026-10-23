//! Module: shell SQL routing.
//! Responsibility: classify shell SQL text before endpoint dispatch.
//! Does not own: ICP execution, response decoding, or SQL parsing semantics.
//! Boundary: exposes routing decisions to the shell runner and test-only shell wrappers.

use icydb::{
    db::{SqlStatementShellSurface, sql_statement_shell_surface},
    diagnostic::ErrorCode,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlShellCallKind {
    Query,
    Ddl,
    Update,
}

pub(super) fn sql_shell_call_kind(sql: &str) -> Result<SqlShellCallKind, String> {
    let surface = sql_statement_shell_surface(sql).map_err(|error| {
        let error = icydb::Error::from(error);
        match error.code() {
            ErrorCode::SQL_SURFACE_QUERY_REJECTS_INSERT
            | ErrorCode::SQL_SURFACE_QUERY_REJECTS_DELETE => format!(
                "{error}: the SQL shell supports queries, DDL and opt-in UPDATE; INSERT and DELETE require an application-owned mutation endpoint"
            ),
            _ => error.to_string(),
        }
    })?;
    match surface {
        SqlStatementShellSurface::Query => Ok(SqlShellCallKind::Query),
        SqlStatementShellSurface::Ddl => Ok(SqlShellCallKind::Ddl),
        SqlStatementShellSurface::Update => Ok(SqlShellCallKind::Update),
    }
}
