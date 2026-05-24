//! Module: shell SQL routing.
//! Responsibility: classify shell SQL text as query or DDL before endpoint dispatch.
//! Does not own: ICP execution, response decoding, or SQL parsing semantics.
//! Boundary: exposes routing decisions to the shell runner and test-only shell wrappers.

use icydb::db::{SqlStatementSurface, sql_statement_surface};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlShellCallKind {
    Query,
    Ddl,
}

pub(super) fn sql_shell_call_kind(sql: &str) -> Result<SqlShellCallKind, String> {
    match sql_statement_surface(sql).map_err(|err| err.to_string())? {
        SqlStatementSurface::Query => Ok(SqlShellCallKind::Query),
        SqlStatementSurface::Ddl => Ok(SqlShellCallKind::Ddl),
    }
}
