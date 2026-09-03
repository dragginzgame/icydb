//! Module: db::session::sql::surface
//! Responsibility: classify parsed SQL statements for generated/controller
//! endpoint routing.
//! Does not own: SQL execution, SQL compilation cache, or DDL publication.
//! Boundary: keeps query/mutation/DDL surface gating out of the SQL facade.

#[cfg(feature = "sql")]
use crate::db::sql::parser::SqlExplainTarget;
use crate::{
    db::{
        DbSession, QueryError,
        session::sql::SqlCompiledCommandSurface,
        sql::parser::{SqlDdlStatement, SqlStatement, parse_sql},
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{SqlLoweringCode, SqlSurfaceMismatchCode};

/// Parsed SQL endpoint surface used by generated SQL helper dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlStatementSurface {
    /// SQL routed to the generated query endpoint.
    ///
    /// Row-mutation statements route here for read-only surface rejection
    /// until a generated write endpoint explicitly selects an update policy.
    Query,
    /// SQL handled by the generated DDL endpoint.
    Ddl,
}

/// Parsed SQL shell call route used by host tooling endpoint dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum SqlStatementShellSurface {
    /// SQL routed to the generated query endpoint.
    Query,
    /// SQL routed to the generated DDL endpoint.
    Ddl,
    /// SQL routed to the generated primary-key-policy update endpoint.
    Update,
}

/// Parsed SQL dispatch facts used by generated query endpoint glue.
///
/// The artifact binds its syntax tree to the exact borrowed input used by the
/// compiled-command cache. Downstream consumers cannot pair one statement
/// with another statement's cache text.
#[doc(hidden)]
pub struct SqlStatementDispatch<'sql> {
    sql: &'sql str,
    statement: SqlStatement,
}

impl<'sql> SqlStatementDispatch<'sql> {
    #[must_use]
    const fn new(sql: &'sql str, statement: SqlStatement) -> Self {
        Self { sql, statement }
    }

    /// Return whether this statement belongs to the operational introspection family.
    #[must_use]
    pub const fn requires_introspection(&self) -> bool {
        sql_statement_requires_introspection_from_statement(&self.statement)
    }

    // Return the exact text from which `statement` was parsed. This is the sole
    // raw-SQL source for the downstream compiled-command cache key.
    pub(in crate::db::session::sql) const fn sql(&self) -> &'sql str {
        self.sql
    }

    pub(in crate::db::session::sql) const fn statement(&self) -> &SqlStatement {
        &self.statement
    }
}

/// Return the entity identifier targeted by one reduced SQL statement.
///
/// `SHOW ENTITIES`, `SHOW STORES`, and `SHOW MEMORY` intentionally have no
/// entity target; callers that dispatch across canister-owned entities may
/// route them through any accepted entity.
#[doc(hidden)]
pub fn sql_statement_entity_name(sql: &str) -> Result<Option<String>, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_entity_name_from_statement(&statement).map(str::to_string))
}

/// Return the generated endpoint surface required by one reduced SQL statement.
#[doc(hidden)]
pub fn sql_statement_surface(sql: &str) -> Result<SqlStatementSurface, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_surface_from_statement(&statement))
}

/// Return the generated endpoint route required by one shell SQL statement.
#[doc(hidden)]
pub fn sql_statement_shell_surface(sql: &str) -> Result<SqlStatementShellSurface, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_shell_surface_from_statement(&statement))
}

/// Return generated query-endpoint routing facts for one reduced SQL statement.
#[doc(hidden)]
pub fn sql_statement_dispatch(sql: &str) -> Result<SqlStatementDispatch<'_>, QueryError> {
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(SqlStatementDispatch::new(sql, statement))
}

const fn sql_statement_surface_from_statement(statement: &SqlStatement) -> SqlStatementSurface {
    match statement {
        SqlStatement::Ddl(_) => SqlStatementSurface::Ddl,
        SqlStatement::Select(_)
        | SqlStatement::Delete(_)
        | SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Describe(_)
        | SqlStatement::ShowConstraints(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowRelations(_)
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => SqlStatementSurface::Query,
        #[cfg(feature = "sql")]
        SqlStatement::Explain(_) => SqlStatementSurface::Query,
    }
}

const fn sql_statement_shell_surface_from_statement(
    statement: &SqlStatement,
) -> SqlStatementShellSurface {
    match statement {
        SqlStatement::Ddl(_) => SqlStatementShellSurface::Ddl,
        SqlStatement::Update(_) => SqlStatementShellSurface::Update,
        SqlStatement::Select(_)
        | SqlStatement::Delete(_)
        | SqlStatement::Insert(_)
        | SqlStatement::Describe(_)
        | SqlStatement::ShowConstraints(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowRelations(_)
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => SqlStatementShellSurface::Query,
        #[cfg(feature = "sql")]
        SqlStatement::Explain(_) => SqlStatementShellSurface::Query,
    }
}

const fn sql_statement_requires_introspection_from_statement(statement: &SqlStatement) -> bool {
    match statement {
        #[cfg(feature = "sql")]
        SqlStatement::Explain(_) => true,
        SqlStatement::Describe(_)
        | SqlStatement::ShowConstraints(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowRelations(_)
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => true,
        SqlStatement::Select(_)
        | SqlStatement::Delete(_)
        | SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Ddl(_) => false,
    }
}

pub(in crate::db::session::sql) const fn sql_statement_entity_name_from_statement(
    statement: &SqlStatement,
) -> Option<&str> {
    match statement {
        SqlStatement::Select(statement) => Some(statement.entity.as_str()),
        SqlStatement::Delete(statement) => Some(statement.entity.as_str()),
        SqlStatement::Insert(statement) => Some(statement.entity.as_str()),
        SqlStatement::Update(statement) => Some(statement.entity.as_str()),
        SqlStatement::Ddl(SqlDdlStatement::CreateIndex(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::DropIndex(statement)) => match &statement.entity {
            Some(entity) => Some(entity.as_str()),
            None => None,
        },
        SqlStatement::Ddl(SqlDdlStatement::AlterTableAddColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableAddCheckConstraint(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableAlterColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableDropColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableDropConstraint(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableRenameColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableValidateConstraint(statement)) => {
            Some(statement.entity.as_str())
        }
        #[cfg(feature = "sql")]
        SqlStatement::Explain(statement) => match &statement.statement {
            SqlExplainTarget::Select(statement) => Some(statement.entity.as_str()),
            SqlExplainTarget::Delete(statement) => Some(statement.entity.as_str()),
        },
        SqlStatement::Describe(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowConstraints(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowIndexes(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowColumns(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowRelations(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => None,
    }
}

impl<C: CanisterKind> DbSession<C> {
    // Keep query/mutation surface gating owned by one helper so the SQL
    // compiled-command lane does not duplicate the same statement-family split
    // just to change the outward error wording.
    pub(in crate::db::session::sql) fn ensure_sql_statement_supported_for_surface(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
    ) -> Result<(), QueryError> {
        match (surface, statement) {
            (
                SqlCompiledCommandSurface::Query,
                SqlStatement::Select(_)
                | SqlStatement::Describe(_)
                | SqlStatement::ShowConstraints(_)
                | SqlStatement::ShowIndexes(_)
                | SqlStatement::ShowColumns(_)
                | SqlStatement::ShowRelations(_)
                | SqlStatement::ShowEntities(_)
                | SqlStatement::ShowStores(_)
                | SqlStatement::ShowMemory(_),
            ) => Ok(()),
            #[cfg(feature = "sql")]
            (SqlCompiledCommandSurface::Query, SqlStatement::Explain(_)) => Ok(()),
            (
                SqlCompiledCommandSurface::Mutation,
                SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_),
            ) => Ok(()),
            (_, SqlStatement::Ddl(_)) => Err(QueryError::sql_lowering(
                SqlLoweringCode::SqlDdlExecutionUnsupported,
            )),
            (SqlCompiledCommandSurface::Query, SqlStatement::Insert(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::QueryRejectsInsert),
            ),
            (SqlCompiledCommandSurface::Query, SqlStatement::Update(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::QueryRejectsUpdate),
            ),
            (SqlCompiledCommandSurface::Query, SqlStatement::Delete(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::QueryRejectsDelete),
            ),
            (SqlCompiledCommandSurface::Mutation, SqlStatement::Select(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::MutationRejectsSelect),
            ),
            #[cfg(feature = "sql")]
            (SqlCompiledCommandSurface::Mutation, SqlStatement::Explain(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::MutationRejectsExplain),
            ),
            (SqlCompiledCommandSurface::Mutation, SqlStatement::Describe(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::MutationRejectsDescribe),
            ),
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowConstraints(_)) => {
                Err(QueryError::sql_surface_mismatch(
                    SqlSurfaceMismatchCode::MutationRejectsShowConstraints,
                ))
            }
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowIndexes(_)) => {
                Err(QueryError::sql_surface_mismatch(
                    SqlSurfaceMismatchCode::MutationRejectsShowIndexes,
                ))
            }
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowColumns(_)) => {
                Err(QueryError::sql_surface_mismatch(
                    SqlSurfaceMismatchCode::MutationRejectsShowColumns,
                ))
            }
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowRelations(_)) => {
                Err(QueryError::sql_surface_mismatch(
                    SqlSurfaceMismatchCode::MutationRejectsShowRelations,
                ))
            }
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowEntities(_)) => {
                Err(QueryError::sql_surface_mismatch(
                    SqlSurfaceMismatchCode::MutationRejectsShowEntities,
                ))
            }
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowStores(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::MutationRejectsShowStores),
            ),
            (SqlCompiledCommandSurface::Mutation, SqlStatement::ShowMemory(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::MutationRejectsShowMemory),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::sql_statement_dispatch;

    #[test]
    fn frozen_operational_sql_families_require_introspection() {
        for sql in [
            "DESCRIBE Example",
            "SHOW CONSTRAINTS FROM Example",
            "SHOW INDEXES FROM Example",
            "SHOW COLUMNS Example",
            "SHOW RELATIONS FROM Example",
            "SHOW ENTITIES",
            "SHOW STORES",
            "SHOW MEMORY",
        ] {
            let dispatch = sql_statement_dispatch(sql)
                .unwrap_or_else(|error| panic!("{sql} should classify: {error}"));
            assert!(dispatch.requires_introspection(), "{sql}");
        }

        let select_sql = "SELECT id FROM Example";
        let select = sql_statement_dispatch(select_sql).expect("ordinary SELECT should classify");
        assert!(!select.requires_introspection());
        assert_eq!(select.sql(), select_sql);
    }

    #[cfg(feature = "sql")]
    #[test]
    fn explain_requires_introspection() {
        let dispatch = sql_statement_dispatch("EXPLAIN SELECT id FROM Example")
            .expect("EXPLAIN should classify");

        assert!(dispatch.requires_introspection());
    }
}
