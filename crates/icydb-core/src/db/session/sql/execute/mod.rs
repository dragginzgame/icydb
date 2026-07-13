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

#[cfg(feature = "sql-explain")]
use crate::db::executor::EntityAuthority;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
#[cfg(feature = "sql-explain")]
use crate::db::sql::lowering::LoweredSqlCommand;
#[cfg(test)]
use crate::db::{QueryAdmissionRejection, query::admission::QueryAdmissionPolicy};
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

#[cfg(test)]
fn query_read_admission_error(rejection: QueryAdmissionRejection) -> QueryError {
    QueryError::from(rejection.code())
}

impl<C: CanisterKind> DbSession<C> {
    fn ensure_sql_execution_context_is_current<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.ensure_accepted_schema_authority_is_current::<E>(
            context.accepted_catalog().enum_catalog_handle().authority(),
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
    pub(in crate::db) fn execute_compiled_sql_context_with_phase_attribution<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C>,
    {
        self.ensure_sql_execution_context_is_current::<E>(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context_phase_attribution::<E>(query, context)
            }
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(lowered) => {
                Self::execute_non_select_compiled_sql_with_phase_attribution_from_executor(
                    context.command(),
                    || {
                        self.execute_explain_sql_with_catalog_cache_attribution::<E>(
                            lowered,
                            context.accepted_catalog(),
                            context.accepted_authority(),
                        )
                    },
                )
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_phase_attribution::<E>(
                    context.command(),
                    command,
                    context.accepted_catalog(),
                ),
            compiled => Self::execute_non_select_compiled_sql_with_phase_attribution_from_executor(
                compiled,
                || {
                    self.execute_compiled_sql_with_catalog_cache_attribution::<E>(
                        compiled,
                        context.accepted_catalog(),
                        context.surface(),
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
        let (authority, schema_info) = catalog
            .accepted_or_provided_entity_authority_and_schema_info_for::<E>(accepted_authority)
            .map_err(QueryError::execute)?;

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
                self.execute_global_aggregate_statement_ref::<E>(command)
            }
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(lowered) => {
                self.execute_explain_sql_with_cache_attribution::<E>(lowered)
            }
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..)
            | CompiledSqlCommand::DescribeEntity
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
                self.execute_select_compiled_sql_with_context::<E>(query, context)
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
            self.execute_metadata_compiled_sql_with_catalog_cache::<E>(compiled, catalog)
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
                .execute_global_aggregate_compiled_statement_ref_with_catalog::<E>(
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

    #[cfg(test)]
    pub(in crate::db::session::sql) fn execute_compiled_sql_context_with_read_admission_policy<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
        policy: &QueryAdmissionPolicy,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.ensure_sql_execution_context_is_current::<E>(context)?;

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                let (result, _) = self
                    .execute_select_compiled_sql_with_context_and_read_admission_policy::<E>(
                        query, context, policy,
                    )?;

                Ok(result)
            }
            CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities { .. }
            | CompiledSqlCommand::ShowStores { .. }
            | CompiledSqlCommand::ShowMemory => Err(query_read_admission_error(
                QueryAdmissionRejection::IntrospectionDisabledForLane,
            )),
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_read_admission_policy::<E>(
                    context.command(),
                    command,
                    policy,
                ),
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..) => Err(query_read_admission_error(
                QueryAdmissionRejection::UnsupportedStatementForQueryLane,
            )),
            #[cfg(feature = "sql-explain")]
            CompiledSqlCommand::Explain(_) => Err(query_read_admission_error(
                QueryAdmissionRejection::UnsupportedStatementForQueryLane,
            )),
        }
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
        let (compiled, _, _) = match statement {
            crate::db::sql::parser::SqlStatement::Insert(_)
            | crate::db::sql::parser::SqlStatement::Update(_)
            | crate::db::sql::parser::SqlStatement::Delete(_) => {
                self.compile_sql_update_with_cache_attribution::<E>(sql)?
            }
            crate::db::sql::parser::SqlStatement::Select(_)
            | crate::db::sql::parser::SqlStatement::Describe(_)
            | crate::db::sql::parser::SqlStatement::ShowIndexes(_)
            | crate::db::sql::parser::SqlStatement::ShowColumns(_)
            | crate::db::sql::parser::SqlStatement::ShowEntities(_)
            | crate::db::sql::parser::SqlStatement::ShowStores(_)
            | crate::db::sql::parser::SqlStatement::ShowMemory(_) => {
                self.compile_sql_query_with_cache_attribution::<E>(sql)?
            }
            #[cfg(feature = "sql-explain")]
            crate::db::sql::parser::SqlStatement::Explain(_) => {
                self.compile_sql_query_with_cache_attribution::<E>(sql)?
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
