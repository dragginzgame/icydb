//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while keeping
//! only route and write wiring in child modules.

mod aggregate_plan;
mod aggregate_request;
mod diagnostics;
mod direct_count;
#[cfg(feature = "sql-explain")]
mod explain;
mod global_aggregate;
mod metadata;
mod select;
mod select_plan;
mod write;
mod write_returning;

use crate::db::executor::EntityAuthority;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
#[cfg(feature = "sql-explain")]
use crate::db::sql::lowering::LoweredSqlCommand;
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandExecutionContext,
                SqlCompiledCommandSurface, SqlStatementResult,
            },
        },
    },
    traits::CanisterKind,
};
#[cfg(feature = "diagnostics")]
use diagnostics::measure_scalar_aggregate_execute_phase_with_physical_access;
#[cfg(test)]
use icydb_diagnostic_code::SqlLoweringCode;
use write::execute_compiled_sql_write_with_default_cache;

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

    fn ensure_sql_execution_context_is_current<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.ensure_accepted_schema_authority_is_current::<E>(
            context
                .accepted_catalog()
                .value_catalog_handle()
                .authority(),
        )
        .map_err(QueryError::execute)
    }

    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    #[cfg(test)]
    pub(in crate::db) fn execute_compiled_sql<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (result, _) = self.execute_compiled_sql_with_cache_attribution::<E>(compiled)?;

        Ok(result)
    }

    /// Execute one owned compiled reduced SQL statement into one unified SQL payload.
    #[cfg(test)]
    pub(in crate::db) fn execute_compiled_sql_owned<E>(
        &self,
        compiled: CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (result, _) = self.execute_compiled_sql_with_cache_attribution::<E>(&compiled)?;

        Ok(result)
    }

    // Keep one perf-only execution entrypoint that returns cache attribution
    // together with planner/runtime instruction splits for shell-facing tools.
    #[cfg(feature = "diagnostics")]
    fn execute_non_select_compiled_sql_with_phase_attribution_from_executor(
        compiled: &CompiledSqlCommand,
        execute: impl FnOnce() -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        if matches!(compiled, CompiledSqlCommand::Select { .. }) {
            return Err(QueryError::execute(
                InternalError::query_executor_invariant(),
            ));
        }

        let (
            scalar_aggregate_terminal,
            ((execute_local_instructions, store_local_instructions), result),
        ) = measure_scalar_aggregate_execute_phase_with_physical_access(execute);
        let (result, cache_attribution) = result?;
        let phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
            execute_local_instructions,
            store_local_instructions,
        )
        .with_scalar_aggregate_terminal(scalar_aggregate_terminal);

        Ok((result, cache_attribution, phase_attribution))
    }

    #[cfg(feature = "diagnostics")]
    pub(in crate::db) fn execute_compiled_sql_query_context_with_phase_attribution(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    > {
        self.ensure_sql_query_execution_context_is_current(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context_phase_attribution(query, context)
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_phase_attribution(
                    context.command(),
                    command,
                    context.accepted_catalog(),
                ),
            compiled => Self::execute_non_select_compiled_sql_with_phase_attribution_from_executor(
                compiled,
                || {
                    self.execute_compiled_sql_query_with_catalog_cache_attribution(
                        compiled,
                        context.accepted_catalog(),
                        context.accepted_authority(),
                    )
                },
            ),
        }
    }

    #[cfg(feature = "sql-explain")]
    fn execute_explain_sql_with_cache_attribution<E>(
        &self,
        lowered: &LoweredSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        self.execute_explain_sql_with_catalog_cache_attribution::<E>(lowered, &catalog, None)
    }

    #[cfg(feature = "sql-explain")]
    fn execute_explain_sql_with_catalog_cache_attribution<E>(
        &self,
        lowered: &LoweredSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        catalog.debug_assert_matches_entity::<E>();
        self.execute_accepted_explain_sql_with_catalog_cache_attribution(
            lowered,
            catalog,
            accepted_authority,
        )
    }

    #[cfg(feature = "sql-explain")]
    fn execute_accepted_explain_sql_with_catalog_cache_attribution(
        &self,
        lowered: &LoweredSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let authority = catalog
            .accepted_or_provided_entity_authority(accepted_authority)
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info();

        if let Some(explain) = self.explain_lowered_sql_execution_for_authority(
            lowered,
            authority.clone(),
            catalog,
            &schema_info,
        )? {
            return Ok((
                SqlStatementResult::Explain(explain),
                SqlCacheAttribution::default(),
            ));
        }

        self.explain_lowered_sql_for_authority(lowered, authority, catalog, &schema_info)
            .map(SqlStatementResult::Explain)
            .map(|result| (result, SqlCacheAttribution::default()))
    }

    pub(in crate::db) fn execute_compiled_sql_with_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        if let Some(result) = self.execute_metadata_compiled_sql_with_default_cache::<E>(compiled) {
            return result;
        }
        if let Some(result) =
            execute_compiled_sql_write_with_default_cache::<E, C>(self, compiled, None, None)
        {
            return result;
        }

        match compiled {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_cache_attribution::<E>(query)
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => {
                self.execute_global_aggregate_statement_ref(command)
            }
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(lowered) => {
                self.execute_explain_sql_with_cache_attribution::<E>(lowered)
            }
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..)
            | CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowConstraintsEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities { .. }
            | CompiledSqlCommand::ShowStores { .. }
            | CompiledSqlCommand::ShowMemory => Err(QueryError::execute(
                InternalError::query_executor_invariant(),
            )),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_context_with_cache_attribution<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.ensure_sql_execution_context_is_current::<E>(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context(query, context)
            }
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(lowered) => self
                .execute_explain_sql_with_catalog_cache_attribution::<E>(
                    lowered,
                    context.accepted_catalog(),
                    context.accepted_authority(),
                ),
            compiled => self.execute_compiled_sql_with_catalog_cache_attribution::<E>(
                compiled,
                context.accepted_catalog(),
                context.surface(),
            ),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_query_context_with_cache_attribution(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
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
            compiled => self.execute_compiled_sql_query_with_catalog_cache_attribution(
                compiled,
                context.accepted_catalog(),
                context.accepted_authority(),
            ),
        }
    }

    fn execute_compiled_sql_query_with_catalog_cache_attribution(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        #[cfg(not(feature = "sql-explain"))]
        let _ = accepted_authority;

        if let Some(result) =
            self.execute_accepted_metadata_compiled_sql_with_catalog_cache(compiled, catalog)
        {
            return result;
        }

        #[cfg(feature = "sql-explain")]
        if let CompiledSqlCommand::Explain(lowered) = compiled {
            return self.execute_accepted_explain_sql_with_catalog_cache_attribution(
                lowered,
                catalog,
                accepted_authority,
            );
        }

        Err(QueryError::execute(
            InternalError::query_executor_invariant(),
        ))
    }

    fn execute_compiled_sql_with_catalog_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        surface: SqlCompiledCommandSurface,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        if let Some(result) =
            self.execute_accepted_metadata_compiled_sql_with_catalog_cache(compiled, catalog)
        {
            return result;
        }
        if let Some(result) = execute_compiled_sql_write_with_default_cache::<E, C>(
            self,
            compiled,
            Some(catalog),
            Some(surface),
        ) {
            return result;
        }

        match compiled {
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_catalog(
                    compiled, command, catalog,
                ),
            _ => self.execute_compiled_sql_with_cache_attribution::<E>(compiled),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_context_owned<E>(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (result, _) =
            self.execute_compiled_sql_context_with_cache_attribution::<E>(&context)?;

        Ok(result)
    }

    pub(in crate::db) fn execute_compiled_sql_query_context_owned(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let (result, _) =
            self.execute_compiled_sql_query_context_with_cache_attribution(&context)?;

        Ok(result)
    }

    /// Compile and then execute one parsed reduced SQL statement into one
    /// unified SQL payload for session-owned tests.
    #[cfg(test)]
    pub(in crate::db) fn execute_sql_statement_inner<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let statement = crate::db::session::sql::parse_sql_statement(sql)?;
        let entity_name = crate::db::session::sql::sql_statement_entity_name(sql)?;
        let (compiled, _, _) = match statement {
            crate::db::sql::parser::SqlStatement::Insert(_)
            | crate::db::sql::parser::SqlStatement::Delete(_) => {
                self.compile_sql_mutation_with_cache_attribution::<E>(sql)?
            }
            crate::db::sql::parser::SqlStatement::Update(_) => {
                return Err(QueryError::sql_surface_mismatch(
                    icydb_diagnostic_code::SqlSurfaceMismatchCode::MutationRequiresExplicitUpdateIntent,
                ));
            }
            crate::db::sql::parser::SqlStatement::Select(_)
            | crate::db::sql::parser::SqlStatement::Describe(_)
            | crate::db::sql::parser::SqlStatement::ShowConstraints(_)
            | crate::db::sql::parser::SqlStatement::ShowIndexes(_)
            | crate::db::sql::parser::SqlStatement::ShowColumns(_)
            | crate::db::sql::parser::SqlStatement::ShowEntities(_)
            | crate::db::sql::parser::SqlStatement::ShowStores(_)
            | crate::db::sql::parser::SqlStatement::ShowMemory(_) => {
                self.compile_sql_query_with_cache_attribution(entity_name.as_deref(), sql)?
            }
            #[cfg(feature = "sql-explain")]
            crate::db::sql::parser::SqlStatement::Explain(_) => {
                self.compile_sql_query_with_cache_attribution(entity_name.as_deref(), sql)?
            }
            crate::db::sql::parser::SqlStatement::Ddl(_) => {
                return Err(QueryError::sql_lowering(
                    SqlLoweringCode::SqlDdlExecutionUnsupported,
                ));
            }
        };

        self.execute_compiled_sql_owned::<E>(compiled)
    }
}
