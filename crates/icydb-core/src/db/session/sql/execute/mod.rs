//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while keeping
//! only route and write wiring in child modules.

mod aggregate_plan;
mod aggregate_request;
mod exact_aggregate;
#[cfg(feature = "sql")]
mod explain;
mod global_aggregate;
mod metadata;
mod select;
mod select_plan;
mod write;
mod write_returning;

use crate::db::executor::EntityAuthority;
#[cfg(feature = "sql")]
use crate::db::sql::lowering::LoweredSqlCommand;
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, QueryError,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCompiledCommandExecutionContext, SqlCompiledCommandSurface,
                SqlStatementResult,
            },
        },
    },
    traits::CanisterKind,
};
use write::execute_compiled_sql_write;

impl<C: CanisterKind> DbSession<C> {
    fn ensure_sql_query_execution_context_is_current(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(), QueryError> {
        self.ensure_accepted_schema_authority_is_current_for_store_path(
            context.accepted_catalog().identity().store_path(),
            context
                .accepted_catalog()
                .value_catalog_handle()
                .authority(),
        )
        .map_err(QueryError::execute)
    }

    #[cfg(feature = "sql")]
    fn execute_accepted_explain_sql_with_catalog(
        &self,
        lowered: &LoweredSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<SqlStatementResult, QueryError> {
        let authority = catalog.accepted_or_provided_entity_authority(accepted_authority);
        let schema_info = catalog.accepted_schema_info();

        if let Some(explain) = self.explain_lowered_sql_execution_for_authority(
            lowered,
            authority.clone(),
            catalog,
            schema_info,
        )? {
            return Ok(SqlStatementResult::Explain(explain));
        }

        self.explain_lowered_sql_for_authority(lowered, authority, catalog, schema_info)
            .map(SqlStatementResult::Explain)
    }

    pub(in crate::db) fn execute_compiled_sql_context(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        self.ensure_sql_query_execution_context_is_current(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context(query, context)
            }
            #[cfg(feature = "sql")]
            CompiledSqlCommand::Explain(lowered) => self.execute_accepted_explain_sql_with_catalog(
                lowered,
                context.accepted_catalog(),
                context.accepted_authority(),
            ),
            compiled => self.execute_compiled_sql_with_catalog(
                compiled,
                context.accepted_catalog(),
                context.surface(),
            ),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_query_context(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        self.ensure_sql_query_execution_context_is_current(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context(query, context)
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_catalog(
                    context.command(),
                    command,
                    context.accepted_catalog(),
                ),
            compiled => self.execute_compiled_sql_query_with_catalog(
                compiled,
                context.accepted_catalog(),
                context.accepted_authority(),
            ),
        }
    }

    fn execute_compiled_sql_query_with_catalog(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<SqlStatementResult, QueryError> {
        #[cfg(not(feature = "sql"))]
        let _ = accepted_authority;

        if let Some(result) =
            self.execute_accepted_metadata_compiled_sql_with_catalog_cache(compiled, catalog)
        {
            return result;
        }

        #[cfg(feature = "sql")]
        if let CompiledSqlCommand::Explain(lowered) = compiled {
            return self.execute_accepted_explain_sql_with_catalog(
                lowered,
                catalog,
                accepted_authority,
            );
        }

        Err(QueryError::execute(
            InternalError::query_executor_invariant(),
        ))
    }

    fn execute_compiled_sql_with_catalog(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        surface: SqlCompiledCommandSurface,
    ) -> Result<SqlStatementResult, QueryError> {
        if let Some(result) =
            self.execute_accepted_metadata_compiled_sql_with_catalog_cache(compiled, catalog)
        {
            return result;
        }
        if let Some(result) =
            execute_compiled_sql_write::<C>(self, compiled, Some(catalog), Some(surface))
        {
            return result;
        }

        match compiled {
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_catalog(
                    compiled, command, catalog,
                ),
            _ => Err(QueryError::execute(
                InternalError::query_executor_invariant(),
            )),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_context_owned(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        self.execute_compiled_sql_context(&context)
    }

    pub(in crate::db) fn execute_compiled_sql_query_context_owned(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        self.execute_compiled_sql_query_context(&context)
    }
}
