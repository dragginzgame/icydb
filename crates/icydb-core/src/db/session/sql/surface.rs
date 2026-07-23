//! Module: db::session::sql::surface
//! Responsibility: classify parsed SQL statements for generated/controller
//! endpoint routing.
//! Does not own: SQL execution, SQL compilation cache, or DDL publication.
//! Boundary: keeps query/mutation/DDL surface gating out of the SQL facade.

#[cfg(feature = "sql-explain")]
use crate::db::sql::parser::SqlExplainTarget;
use crate::{
    db::{
        DbSession, QueryError,
        session::sql::SqlCompiledCommandSurface,
        sql::parser::{SqlDdlStatement, SqlStatement, parse_sql_with_attribution},
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct SqlStatementDispatch {
    entity_name: Option<String>,
    requires_introspection: bool,
}

impl SqlStatementDispatch {
    #[must_use]
    const fn new(entity_name: Option<String>, requires_introspection: bool) -> Self {
        Self {
            entity_name,
            requires_introspection,
        }
    }

    /// Return the entity targeted by this statement, when the SQL family has one.
    #[must_use]
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Return whether this statement belongs to the operational introspection family.
    #[must_use]
    pub const fn requires_introspection(&self) -> bool {
        self.requires_introspection
    }
}

/// Return the entity identifier targeted by one reduced SQL statement.
///
/// `SHOW ENTITIES`, `SHOW STORES`, and `SHOW MEMORY` intentionally have no
/// entity target; callers that dispatch across canister-owned entities may
/// route them through any accepted entity.
#[doc(hidden)]
pub fn sql_statement_entity_name(sql: &str) -> Result<Option<String>, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_entity_name_from_statement(&statement).map(str::to_string))
}

/// Return the generated endpoint surface required by one reduced SQL statement.
#[doc(hidden)]
pub fn sql_statement_surface(sql: &str) -> Result<SqlStatementSurface, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_surface_from_statement(&statement))
}

/// Return the generated endpoint route required by one shell SQL statement.
#[doc(hidden)]
pub fn sql_statement_shell_surface(sql: &str) -> Result<SqlStatementShellSurface, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(sql_statement_shell_surface_from_statement(&statement))
}

/// Return generated query-endpoint routing facts for one reduced SQL statement.
#[doc(hidden)]
pub fn sql_statement_dispatch(sql: &str) -> Result<SqlStatementDispatch, QueryError> {
    let (statement, _) =
        parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;

    Ok(SqlStatementDispatch::new(
        sql_statement_entity_name_from_statement(&statement).map(str::to_string),
        sql_statement_requires_introspection_from_statement(&statement),
    ))
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
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => SqlStatementSurface::Query,
        #[cfg(feature = "sql-explain")]
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
        | SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => SqlStatementShellSurface::Query,
        #[cfg(feature = "sql-explain")]
        SqlStatement::Explain(_) => SqlStatementShellSurface::Query,
    }
}

const fn sql_statement_requires_introspection_from_statement(statement: &SqlStatement) -> bool {
    match statement {
        #[cfg(feature = "sql-explain")]
        SqlStatement::Explain(_) => true,
        SqlStatement::Describe(_)
        | SqlStatement::ShowConstraints(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
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

const fn sql_statement_entity_name_from_statement(statement: &SqlStatement) -> Option<&str> {
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
        #[cfg(feature = "sql-explain")]
        SqlStatement::Explain(statement) => match &statement.statement {
            SqlExplainTarget::Select(statement) => Some(statement.entity.as_str()),
            SqlExplainTarget::Delete(statement) => Some(statement.entity.as_str()),
        },
        SqlStatement::Describe(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowConstraints(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowIndexes(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowColumns(statement) => Some(statement.entity.as_str()),
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
                | SqlStatement::ShowEntities(_)
                | SqlStatement::ShowStores(_)
                | SqlStatement::ShowMemory(_),
            ) => Ok(()),
            #[cfg(feature = "sql-explain")]
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
            #[cfg(feature = "sql-explain")]
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
