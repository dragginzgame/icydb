//! Module: shell SQL routing.
//! Responsibility: classify shell SQL text before endpoint dispatch.
//! Does not own: ICP execution, response decoding, or SQL parsing semantics.
//! Boundary: exposes routing decisions to the shell runner and test-only shell wrappers.

use icydb::db::{SqlStatementShellSurface, sql_statement_shell_surface};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlShellCallKind {
    Query,
    Ddl,
    Update,
}

pub(super) fn sql_shell_call_kind(sql: &str) -> Result<SqlShellCallKind, String> {
    match sql_statement_shell_surface(sql).map_err(|err| err.to_string())? {
        SqlStatementShellSurface::Query => Ok(SqlShellCallKind::Query),
        SqlStatementShellSurface::Ddl => Ok(SqlShellCallKind::Ddl),
        SqlStatementShellSurface::Update => Ok(SqlShellCallKind::Update),
    }
}
