//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while keeping
//! only route and write wiring in child modules.

mod diagnostics;
mod explain;
mod global_aggregate;
mod metadata;
mod select;
mod write;
mod write_returning;

#[cfg(feature = "diagnostics")]
use crate::db::executor::with_scalar_aggregate_terminal_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
use crate::error::InternalError;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::EntityAuthority,
        response::ResponseError,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlCacheAttribution, SqlCompiledCommandExecutionContext,
                SqlStatementResult,
            },
        },
        sql::lowering::LoweredSqlCommand,
        sql::parser::{SqlInsertSource, SqlInsertStatement},
    },
    error::ErrorClass,
    metrics::sink::{MetricsEvent, SqlWriteKind, record},
    traits::{CanisterKind, EntityValue},
};
#[cfg(feature = "diagnostics")]
use diagnostics::measure_execute_phase_with_physical_access;
#[cfg(test)]
use icydb_diagnostic_code::SqlLoweringCode;

// Collapse SQL execution failures into the stable error taxonomy used by the
// public metrics report instead of exposing internal query-error variants.
const fn sql_write_error_class(error: &QueryError) -> ErrorClass {
    match error {
        QueryError::Execute(err) => err.as_internal().class(),
        QueryError::Response(ResponseError::NotFound { .. }) => ErrorClass::NotFound,
        QueryError::Response(ResponseError::NotUnique { .. }) => ErrorClass::Conflict,
        QueryError::Validate(_)
        | QueryError::Plan(_)
        | QueryError::Intent(_)
        | QueryError::AccessRequirement(_) => ErrorClass::Unsupported,
    }
}

// Preserve the important INSERT shape distinction because `INSERT ... SELECT`
// has very different execution and debugging characteristics from VALUES.
const fn sql_insert_write_kind(statement: &SqlInsertStatement) -> SqlWriteKind {
    match &statement.source {
        SqlInsertSource::Values(_) => SqlWriteKind::Insert,
        SqlInsertSource::Select(_) => SqlWriteKind::InsertSelect,
    }
}

// Record only rejected SQL writes at the statement boundary. Successful writes
// are counted by the write executors after they know row cardinalities.
fn record_sql_write_error<E, C>(kind: SqlWriteKind, result: &Result<SqlStatementResult, QueryError>)
where
    E: PersistedRow<Canister = C> + EntityValue,
    C: CanisterKind,
{
    if let Err(error) = result {
        record(MetricsEvent::SqlWriteError {
            entity_path: E::PATH,
            kind,
            class: sql_write_error_class(error),
        });
    }
}

fn sql_statement_result_with_default_cache(
    result: Result<SqlStatementResult, QueryError>,
) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
    result.map(|result| (result, SqlCacheAttribution::default()))
}

fn sql_write_statement_result_with_default_cache<E, C>(
    kind: SqlWriteKind,
    result: Result<SqlStatementResult, QueryError>,
) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
where
    E: PersistedRow<Canister = C> + EntityValue,
    C: CanisterKind,
{
    record_sql_write_error::<E, C>(kind, &result);
    sql_statement_result_with_default_cache(result)
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one compiled reduced SQL statement into one unified SQL payload.
    #[cfg(test)]
    pub(in crate::db) fn execute_compiled_sql<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) = self.execute_compiled_sql_with_cache_attribution::<E>(compiled)?;

        Ok(result)
    }

    /// Execute one owned compiled reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_compiled_sql_owned<E>(
        &self,
        compiled: CompiledSqlCommand,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) = self.execute_compiled_sql_owned_with_cache_attribution::<E>(compiled)?;

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
        ) = with_scalar_aggregate_terminal_attribution(|| {
            measure_execute_phase_with_physical_access(execute)
        });
        let (result, cache_attribution) = result?;
        let mut phase_attribution = SqlExecutePhaseAttribution::from_execute_total_and_store_total(
            execute_local_instructions,
            store_local_instructions,
        );
        phase_attribution.scalar_aggregate_terminal = scalar_aggregate_terminal;

        Ok((result, cache_attribution, phase_attribution))
    }

    #[cfg(feature = "diagnostics")]
    fn execute_non_select_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Self::execute_non_select_compiled_sql_with_phase_attribution_from_executor(compiled, || {
            self.execute_compiled_sql_with_cache_attribution::<E>(compiled)
        })
    }

    #[cfg(feature = "diagnostics")]
    fn execute_non_select_compiled_sql_with_phase_attribution_from_catalog<E>(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        Self::execute_non_select_compiled_sql_with_phase_attribution_from_executor(compiled, || {
            self.execute_compiled_sql_with_catalog_cache_attribution::<E>(compiled, catalog)
        })
    }

    // Execute one compiled SQL command while preserving diagnostics-only
    // cache, planning, executor, and response-finalization phase attribution
    // at the session/executor handoff.
    #[cfg(feature = "diagnostics")]
    #[expect(
        dead_code,
        reason = "explicit compiled SQL diagnostics can still enter without a compile context; query endpoint diagnostics use the context-aware sibling"
    )]
    pub(in crate::db) fn execute_compiled_sql_with_phase_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<
        (
            SqlStatementResult,
            SqlCacheAttribution,
            SqlExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match compiled {
            CompiledSqlCommand::Select { query, .. } => self
                .execute_select_compiled_sql_with_phase_attribution_from_resolver::<E>(
                    query,
                    || self.sql_select_prepared_plan_for_entity::<E>(query),
                ),
            CompiledSqlCommand::GlobalAggregate { command, .. } => {
                let catalog = self
                    .accepted_schema_catalog_context_for_query::<E>()
                    .map_err(QueryError::execute)?;

                self.execute_global_aggregate_compiled_statement_ref_with_phase_attribution::<E>(
                    compiled, command, &catalog,
                )
            }
            CompiledSqlCommand::Delete { .. }
            | CompiledSqlCommand::Explain(..)
            | CompiledSqlCommand::Insert(..)
            | CompiledSqlCommand::Update(..)
            | CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities { .. }
            | CompiledSqlCommand::ShowStores { .. }
            | CompiledSqlCommand::ShowMemory => {
                self.execute_non_select_compiled_sql_with_phase_attribution::<E>(compiled)
            }
        }
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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_phase_attribution_from_resolver::<E>(
                    query,
                    || {
                        if let Some((prepared_plan, projection)) =
                            context.command().cached_select_plan(
                                context.schema_fingerprint_method_version(),
                                context.schema_fingerprint(),
                            )
                        {
                            return Ok((
                                prepared_plan,
                                projection,
                                SqlCacheAttribution::shared_query_plan_cache_hit(),
                            ));
                        }

                        let authority = match context.accepted_authority() {
                            Some(authority) => authority.clone(),
                            None => context
                                .accepted_catalog()
                                .accepted_entity_authority_for::<E>()
                                .map_err(QueryError::execute)?,
                        };

                        let resolved = self
                            .sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
                            query,
                            authority,
                            context.accepted_schema(),
                            context.schema_fingerprint(),
                        );
                        if let Ok((prepared_plan, projection, _)) = &resolved {
                            context.command().set_cached_select_plan(
                                context.schema_fingerprint_method_version(),
                                context.schema_fingerprint(),
                                prepared_plan.clone(),
                                projection.clone(),
                            );
                        }

                        resolved
                    },
                )
            }
            CompiledSqlCommand::Explain(lowered) => {
                let (
                    scalar_aggregate_terminal,
                    ((execute_local_instructions, store_local_instructions), result),
                ) = with_scalar_aggregate_terminal_attribution(|| {
                    measure_execute_phase_with_physical_access(|| {
                        self.execute_explain_sql_with_catalog_cache_attribution::<E>(
                            lowered,
                            context.accepted_catalog(),
                            context.accepted_authority(),
                        )
                    })
                });
                let (result, cache_attribution) = result?;
                let mut phase_attribution =
                    SqlExecutePhaseAttribution::from_execute_total_and_store_total(
                        execute_local_instructions,
                        store_local_instructions,
                    );
                phase_attribution.scalar_aggregate_terminal = scalar_aggregate_terminal;

                Ok((result, cache_attribution, phase_attribution))
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => self
                .execute_global_aggregate_compiled_statement_ref_with_phase_attribution::<E>(
                    context.command(),
                    command,
                    context.accepted_catalog(),
                ),
            compiled => self
                .execute_non_select_compiled_sql_with_phase_attribution_from_catalog::<E>(
                    compiled,
                    context.accepted_catalog(),
                ),
        }
    }

    fn execute_explain_sql_with_cache_attribution<E>(
        &self,
        lowered: &LoweredSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        self.execute_explain_sql_with_catalog_cache_attribution::<E>(lowered, &catalog, None)
    }

    fn execute_explain_sql_with_catalog_cache_attribution<E>(
        &self,
        lowered: &LoweredSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
        accepted_authority: Option<&EntityAuthority>,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let authority = match accepted_authority {
            Some(authority) => authority.clone(),
            None => catalog
                .accepted_entity_authority_for::<E>()
                .map_err(QueryError::execute)?,
        };
        let schema_info = catalog.accepted_schema_info_for::<E>();

        if let Some(explain) = self.explain_lowered_sql_execution_for_authority(
            lowered,
            authority.clone(),
            catalog.snapshot(),
            &schema_info,
        )? {
            return Ok((
                SqlStatementResult::Explain(explain),
                SqlCacheAttribution::default(),
            ));
        }

        self.explain_lowered_sql_for_authority(lowered, authority, catalog.snapshot(), &schema_info)
            .map(SqlStatementResult::Explain)
            .map(|result| (result, SqlCacheAttribution::default()))
    }

    pub(in crate::db) fn execute_compiled_sql_with_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(result) = self.execute_metadata_compiled_sql_with_default_cache::<E>(compiled) {
            return result;
        }

        match compiled {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_cache_attribution::<E>(query)
            }
            CompiledSqlCommand::Delete { query, returning } => {
                let result =
                    self.execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref());
                sql_write_statement_result_with_default_cache::<E, C>(SqlWriteKind::Delete, result)
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => {
                self.execute_global_aggregate_statement_ref::<E>(command)
            }
            CompiledSqlCommand::Explain(lowered) => {
                self.execute_explain_sql_with_cache_attribution::<E>(lowered)
            }
            CompiledSqlCommand::Insert(command) => {
                let result = self
                    .execute_sql_insert_statement::<E>(command.statement(), command.source_query());
                sql_write_statement_result_with_default_cache::<E, C>(
                    sql_insert_write_kind(command.statement()),
                    result,
                )
            }
            CompiledSqlCommand::Update(statement) => {
                let result = self.execute_sql_update_statement::<E>(statement);
                sql_write_statement_result_with_default_cache::<E, C>(SqlWriteKind::Update, result)
            }
            CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities { .. }
            | CompiledSqlCommand::ShowStores { .. }
            | CompiledSqlCommand::ShowMemory => unreachable!("metadata SQL handled above"),
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    #[expect(
        dead_code,
        reason = "available for cache-attribution tests over compile contexts; normal query execution uses owned or diagnostics context entrypoints"
    )]
    pub(in crate::db) fn execute_compiled_sql_context_with_cache_attribution<E>(
        &self,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context::<E>(query, context)
            }
            CompiledSqlCommand::Explain(lowered) => self
                .execute_explain_sql_with_catalog_cache_attribution::<E>(
                    lowered,
                    context.accepted_catalog(),
                    context.accepted_authority(),
                ),
            compiled => self.execute_compiled_sql_with_catalog_cache_attribution::<E>(
                compiled,
                context.accepted_catalog(),
            ),
        }
    }

    fn execute_compiled_sql_with_catalog_cache_attribution<E>(
        &self,
        compiled: &CompiledSqlCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(result) =
            self.execute_metadata_compiled_sql_with_catalog_cache::<E>(compiled, catalog)
        {
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

    pub(in crate::db) fn execute_compiled_sql_owned_with_cache_attribution<E>(
        &self,
        compiled: CompiledSqlCommand,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(result) = self.execute_metadata_compiled_sql_with_default_cache::<E>(&compiled)
        {
            return result;
        }

        match compiled {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_cache_attribution::<E>(query.as_ref())
            }
            CompiledSqlCommand::Delete { query, returning } => {
                let result =
                    self.execute_sql_delete_statement::<E>(query.as_ref(), returning.as_ref());
                sql_write_statement_result_with_default_cache::<E, C>(SqlWriteKind::Delete, result)
            }
            CompiledSqlCommand::GlobalAggregate { command, .. } => {
                self.execute_global_aggregate_statement_ref::<E>(&command)
            }
            CompiledSqlCommand::Explain(lowered) => {
                self.execute_explain_sql_with_cache_attribution::<E>(&lowered)
            }
            CompiledSqlCommand::Insert(command) => {
                let kind = sql_insert_write_kind(command.statement());
                let result = self
                    .execute_sql_insert_statement::<E>(command.statement(), command.source_query());
                sql_write_statement_result_with_default_cache::<E, C>(kind, result)
            }
            CompiledSqlCommand::Update(statement) => {
                let result = self.execute_sql_update_statement::<E>(&statement);
                sql_write_statement_result_with_default_cache::<E, C>(SqlWriteKind::Update, result)
            }
            CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities { .. }
            | CompiledSqlCommand::ShowStores { .. }
            | CompiledSqlCommand::ShowMemory => unreachable!("metadata SQL handled above"),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_context_owned_with_cache_attribution<E>(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if let Some(result) = self.execute_metadata_compiled_sql_with_catalog_cache::<E>(
            context.command(),
            context.accepted_catalog(),
        ) {
            return result;
        }

        match context.command() {
            CompiledSqlCommand::Select { query, .. } => {
                self.execute_select_compiled_sql_with_context::<E>(query, &context)
            }
            CompiledSqlCommand::Explain(lowered) => self
                .execute_explain_sql_with_catalog_cache_attribution::<E>(
                    lowered,
                    context.accepted_catalog(),
                    context.accepted_authority(),
                ),
            _ => self.execute_compiled_sql_with_catalog_cache_attribution::<E>(
                context.command(),
                context.accepted_catalog(),
            ),
        }
    }

    pub(in crate::db) fn execute_compiled_sql_context_owned<E>(
        &self,
        context: SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (result, _) =
            self.execute_compiled_sql_context_owned_with_cache_attribution::<E>(context)?;

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
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = crate::db::session::sql::parse_sql_statement(sql)?;
        let (compiled, _, _) = match statement {
            crate::db::sql::parser::SqlStatement::Insert(_)
            | crate::db::sql::parser::SqlStatement::Update(_)
            | crate::db::sql::parser::SqlStatement::Delete(_) => {
                self.compile_sql_update_with_cache_attribution::<E>(sql)?
            }
            crate::db::sql::parser::SqlStatement::Select(_)
            | crate::db::sql::parser::SqlStatement::Explain(_)
            | crate::db::sql::parser::SqlStatement::Describe(_)
            | crate::db::sql::parser::SqlStatement::ShowIndexes(_)
            | crate::db::sql::parser::SqlStatement::ShowColumns(_)
            | crate::db::sql::parser::SqlStatement::ShowEntities(_)
            | crate::db::sql::parser::SqlStatement::ShowStores(_)
            | crate::db::sql::parser::SqlStatement::ShowMemory(_) => {
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
