//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod attribution;
mod cache;
mod compile;
mod compile_cache;
mod compiled;
mod delete_policy;
mod execute;
mod projection;
mod result;
mod update_policy;
mod write_policy;

#[cfg(feature = "diagnostics")]
use crate::db::GroupedExecutionAttribution;
#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::StoreCounterSnapshot;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(feature = "sql-explain")]
use crate::db::sql::parser::SqlExplainTarget;
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
#[cfg(feature = "diagnostics")]
use crate::value::OutputValue;
use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        schema::AcceptedSchemaSnapshot,
        schema::{
            execute_sql_ddl_expression_index_addition, execute_sql_ddl_field_addition,
            execute_sql_ddl_field_default_change, execute_sql_ddl_field_drop,
            execute_sql_ddl_field_nullability_change, execute_sql_ddl_field_path_index_addition,
            execute_sql_ddl_field_rename, execute_sql_ddl_secondary_index_drop,
        },
        session::sql::projection::projection_contract_from_projection_spec,
        session::{AcceptedSchemaCatalogContext, query::QueryPlanCacheAttribution},
        sql::{
            ddl::{PreparedSqlDdlCommand, prepare_sql_ddl_statement},
            parser::{SqlDdlStatement, SqlStatement, parse_sql_with_attribution},
        },
    },
    traits::{CanisterKind, EntityValue, Path},
};
use icydb_diagnostic_code::{SqlLoweringCode, SqlSurfaceMismatchCode};

pub(in crate::db::session::sql) use crate::db::diagnostics::measure_local_instruction_delta as measure_sql_stage;
pub use crate::db::sql::ddl::{SqlDdlExecutionStatus, SqlDdlMutationKind, SqlDdlPreparationReport};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use attribution::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub use attribution::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution, SqlScalarAggregateAttribution,
};
pub(in crate::db) use cache::{SqlCacheAttribution, SqlCompiledCommandCacheKey};
pub(in crate::db::session::sql) use cache::{
    SqlCompiledCommandSurface, sql_compiled_command_cache_miss_reason,
};
pub(in crate::db::session::sql) use compile::{
    SqlCompileAttributionBuilder, SqlCompilePhaseAttribution,
};
pub(in crate::db) use compiled::{
    CompiledSqlCommand, CompiledSqlInsertCommand, SqlCompiledCommandExecutionContext,
    SqlCompiledSchemaFingerprint, SqlGlobalAggregateCountPlanCacheEntry,
};
#[cfg(test)]
pub(in crate::db) use delete_policy::{
    DEFAULT_PUBLIC_BOUNDED_DELETE_LIMIT, DEFAULT_PUBLIC_DELETE_RETURNING_RESPONSE_BYTES,
};
pub use delete_policy::{
    SqlAdminBulkDeletePlan, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
    SqlDeletePolicyRejection, SqlDeletePolicyReport, SqlDeleteStatementClassification,
    SqlPublicBoundedDeletePlan, SqlPublicPrimaryKeyDeletePlan, SqlSessionCurrentDeletePlan,
    SqlValidatedDeletePlan, classify_sql_delete_policy,
};
pub(in crate::db) use projection::SqlProjectionContract;
pub use result::SqlStatementResult;
#[cfg(test)]
pub(in crate::db) use update_policy::{
    DEFAULT_PUBLIC_BOUNDED_UPDATE_LIMIT, DEFAULT_PUBLIC_UPDATE_RETURNING_RESPONSE_BYTES,
};
pub use update_policy::{
    SqlAdminBulkUpdatePlan, SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyUpdatePlan,
    SqlSessionCurrentUpdatePlan, SqlUpdateAssignmentPolicy, SqlUpdateExposurePolicy,
    SqlUpdatePolicyContext, SqlUpdatePolicyRejection, SqlUpdatePolicyReport,
    SqlUpdateStatementClassification, SqlValidatedUpdatePlan, classify_sql_update_policy,
};
pub(in crate::db::session::sql) use write_policy::combined_optional_row_bound;
pub use write_policy::{
    SqlWriteExecutionBounds, SqlWriteOrderProof, SqlWriteReturningBounds, SqlWriteReturningShape,
    SqlWriteStatementShape, SqlWriteWhereProof,
};

#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy)]
struct SqlQueryExecutionAttributionInputs {
    compile_local_instructions: u64,
    compile_phase_attribution: SqlCompilePhaseAttribution,
    compile_cache_attribution: SqlCacheAttribution,
    execute_cache_attribution: SqlCacheAttribution,
    execute_phase_attribution: SqlExecutePhaseAttribution,
    pure_covering_decode_local_instructions: u64,
    pure_covering_row_assembly_local_instructions: u64,
    projection_materialization: SqlProjectionMaterializationMetrics,
    store_counters: StoreCounterSnapshot,
}

#[cfg(feature = "diagnostics")]
const fn sql_execute_local_instructions_from_phase(phase: &SqlExecutePhaseAttribution) -> u64 {
    phase
        .planner_local_instructions
        .saturating_add(phase.store_local_instructions)
        .saturating_add(phase.executor_local_instructions)
        .saturating_add(phase.response_finalization_local_instructions)
}

#[cfg(feature = "diagnostics")]
fn sql_query_execution_attribution_from_inputs(
    result: &SqlStatementResult,
    inputs: &SqlQueryExecutionAttributionInputs,
) -> SqlQueryExecutionAttribution {
    let execute_phase = &inputs.execute_phase_attribution;
    let execute_local_instructions = sql_execute_local_instructions_from_phase(execute_phase);
    let total_local_instructions = inputs
        .compile_local_instructions
        .saturating_add(execute_local_instructions);
    let grouped = matches!(result, SqlStatementResult::Grouped { .. }).then_some(
        GroupedExecutionAttribution::from_executor_parts(
            execute_phase.grouped_stream_local_instructions,
            execute_phase.grouped_fold_local_instructions,
            execute_phase.grouped_finalize_local_instructions,
            execute_phase.grouped_count,
        ),
    );

    SqlQueryExecutionAttribution {
        compile_local_instructions: inputs.compile_local_instructions,
        compile: SqlCompileAttribution::from_phase(inputs.compile_phase_attribution),
        plan_lookup_local_instructions: execute_phase.planner_local_instructions,
        execution: SqlExecutionAttribution::from_phase(execute_phase),
        direct_data_row: execute_phase.direct_data_row,
        kernel_row: execute_phase.kernel_row,
        grouped,
        scalar_aggregate: SqlScalarAggregateAttribution::from_executor(
            execute_phase.scalar_aggregate_terminal,
        ),
        pure_covering: SqlPureCoveringAttribution::from_local_instructions(
            inputs.pure_covering_decode_local_instructions,
            inputs.pure_covering_row_assembly_local_instructions,
        ),
        hybrid_covering: SqlHybridCoveringAttribution::from_projection_metrics(
            inputs.projection_materialization,
        ),
        output_blob: sql_output_blob_attribution(result),
        store_get_calls: inputs.store_counters.data_store_get_calls,
        index_store_get_calls: inputs.store_counters.index_store_get_calls,
        index_store_range_scan_calls: inputs.store_counters.index_store_range_scan_calls,
        index_store_entry_reads: inputs.store_counters.index_store_entry_reads,
        response_decode_local_instructions: 0,
        execute_local_instructions,
        total_local_instructions,
        cache: SqlQueryCacheAttribution::from_phases(
            inputs.compile_cache_attribution,
            inputs.execute_cache_attribution,
        ),
    }
}

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

#[cfg(feature = "diagnostics")]
fn sql_output_blob_attribution(result: &SqlStatementResult) -> SqlOutputBlobAttribution {
    let mut attribution = SqlOutputBlobAttribution::default();

    match result {
        SqlStatementResult::Projection { rows, .. } => {
            for row in rows {
                for value in row {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Grouped { rows, .. } => {
            for row in rows {
                for value in row.group_key().iter().chain(row.aggregate_values()) {
                    record_output_value_blob_attribution(value, &mut attribution);
                }
            }
        }
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities { .. }
        | SqlStatementResult::ShowStores { .. }
        | SqlStatementResult::ShowMemory(_)
        | SqlStatementResult::Ddl(_) => {}
        #[cfg(feature = "sql-explain")]
        SqlStatementResult::Explain(_) => {}
    }

    attribution
}

#[cfg(feature = "diagnostics")]
fn record_output_value_blob_attribution(
    value: &OutputValue,
    attribution: &mut SqlOutputBlobAttribution,
) {
    match value {
        OutputValue::Blob(bytes) => {
            let byte_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            attribution.projected_values = attribution.projected_values.saturating_add(1);
            attribution.projected_bytes = attribution.projected_bytes.saturating_add(byte_len);
            attribution.rendered_hex_bytes = attribution
                .rendered_hex_bytes
                .saturating_add(byte_len.saturating_mul(2).saturating_add(2));
        }
        OutputValue::Enum(value) => {
            if let Some(payload) = value.payload() {
                record_output_value_blob_attribution(payload, attribution);
            }
        }
        OutputValue::List(items) => {
            for item in items {
                record_output_value_blob_attribution(item, attribution);
            }
        }
        OutputValue::Map(entries) => {
            for (key, value) in entries {
                record_output_value_blob_attribution(key, attribution);
                record_output_value_blob_attribution(value, attribution);
            }
        }
        OutputValue::Account(_)
        | OutputValue::Bool(_)
        | OutputValue::Date(_)
        | OutputValue::Decimal(_)
        | OutputValue::Duration(_)
        | OutputValue::Float32(_)
        | OutputValue::Float64(_)
        | OutputValue::Int64(_)
        | OutputValue::Int128(_)
        | OutputValue::IntBig(_)
        | OutputValue::Null
        | OutputValue::Principal(_)
        | OutputValue::Subaccount(_)
        | OutputValue::Text(_)
        | OutputValue::Timestamp(_)
        | OutputValue::Nat64(_)
        | OutputValue::Nat128(_)
        | OutputValue::NatBig(_)
        | OutputValue::Ulid(_)
        | OutputValue::Unit => {}
    }
}

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
#[cfg(test)]
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
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
        SqlStatement::Ddl(SqlDdlStatement::AlterTableAlterColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableDropColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        SqlStatement::Ddl(SqlDdlStatement::AlterTableRenameColumn(statement)) => {
            Some(statement.entity.as_str())
        }
        #[cfg(feature = "sql-explain")]
        SqlStatement::Explain(statement) => match &statement.statement {
            SqlExplainTarget::Select(statement) => Some(statement.entity.as_str()),
            SqlExplainTarget::Delete(statement) => Some(statement.entity.as_str()),
        },
        SqlStatement::Describe(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowIndexes(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowColumns(statement) => Some(statement.entity.as_str()),
        SqlStatement::ShowEntities(_)
        | SqlStatement::ShowStores(_)
        | SqlStatement::ShowMemory(_) => None,
    }
}

// Measure one SQL compile stage and immediately surface the stage result. The
// helper keeps attribution capture uniform while avoiding repeated
// `(cost, result); result?` boilerplate across the compile pipeline.
fn measured<T>(stage: impl FnOnce() -> Result<T, QueryError>) -> Result<(u64, T), QueryError> {
    let (local_instructions, result) = measure_sql_stage(stage);
    let value = result?;

    Ok((local_instructions, value))
}

impl<C: CanisterKind> DbSession<C> {
    // Resolve one SQL SELECT through a caller-selected accepted authority and
    // accepted schema snapshot. Typed SQL entrypoints use this to avoid passing
    // generated authority through the runtime cache boundary.
    fn sql_select_prepared_plan_for_accepted_authority(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let schema_fingerprint =
            crate::db::schema::accepted_schema_cache_fingerprint(accepted_schema)
                .map_err(QueryError::execute)?;

        self.sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
            query,
            authority,
            accepted_schema,
            schema_fingerprint,
        )
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint(
                authority.clone(),
                accepted_schema,
                schema_fingerprint,
                query,
            )?;
        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    fn sql_select_prepared_plan_for_accepted_authority_with_catalog(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority.clone(),
                catalog,
                query,
            )?;
        Ok(Self::sql_select_projection_from_prepared_plan(
            prepared_plan,
            authority,
            cache_attribution,
        ))
    }

    #[cfg(feature = "diagnostics")]
    fn sql_select_prepared_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
            crate::db::session::query::QueryPlanCompilePhaseAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution, plan_compile_attribution) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog_and_compile_phase_attribution(
                authority.clone(),
                catalog,
                query,
            )?;
        let (prepared_plan, projection, cache_attribution) =
            Self::sql_select_projection_from_prepared_plan(
                prepared_plan,
                authority,
                cache_attribution,
            );

        Ok((
            prepared_plan,
            projection,
            cache_attribution,
            plan_compile_attribution,
        ))
    }

    fn sql_select_projection_from_prepared_plan(
        prepared_plan: SharedPreparedExecutionPlan,
        authority: EntityAuthority,
        cache_attribution: QueryPlanCacheAttribution,
    ) -> (
        SharedPreparedExecutionPlan,
        SqlProjectionContract,
        SqlCacheAttribution,
    ) {
        let projection_spec = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let projection = projection_contract_from_projection_spec(&projection_spec);

        (
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        )
    }

    // Keep query/update surface gating owned by one helper so the SQL
    // compiled-command lane does not duplicate the same statement-family split
    // just to change the outward error wording.
    fn ensure_sql_statement_supported_for_surface(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
    ) -> Result<(), QueryError> {
        match (surface, statement) {
            (
                SqlCompiledCommandSurface::Query,
                SqlStatement::Select(_)
                | SqlStatement::Describe(_)
                | SqlStatement::ShowIndexes(_)
                | SqlStatement::ShowColumns(_)
                | SqlStatement::ShowEntities(_)
                | SqlStatement::ShowStores(_)
                | SqlStatement::ShowMemory(_),
            ) => Ok(()),
            #[cfg(feature = "sql-explain")]
            (SqlCompiledCommandSurface::Query, SqlStatement::Explain(_)) => Ok(()),
            (
                SqlCompiledCommandSurface::Update,
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
            (SqlCompiledCommandSurface::Update, SqlStatement::Select(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsSelect),
            ),
            #[cfg(feature = "sql-explain")]
            (SqlCompiledCommandSurface::Update, SqlStatement::Explain(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsExplain),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::Describe(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsDescribe),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowIndexes(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsShowIndexes),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowColumns(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsShowColumns),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowEntities(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsShowEntities),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowStores(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsShowStores),
            ),
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowMemory(_)) => Err(
                QueryError::sql_surface_mismatch(SqlSurfaceMismatchCode::UpdateRejectsShowMemory),
            ),
        }
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (compiled, _, _) = self.compile_sql_query_with_execution_context::<E>(sql)?;

        self.execute_compiled_sql_context_owned::<E>(compiled)
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure the compile side of the new seam, including parse,
        // surface validation, and semantic command construction.
        let (compile_local_instructions, compiled) =
            measure_sql_stage(|| self.compile_sql_query_with_execution_context::<E>(sql));
        let (compiled, compile_cache_attribution, compile_phase_attribution) = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let store_counters_before = StoreCounterSnapshot::capture();
        let pure_covering_decode_before = current_pure_covering_decode_local_instructions();
        let pure_covering_row_assembly_before =
            current_pure_covering_row_assembly_local_instructions();
        let (executed, projection_materialization) =
            with_sql_projection_materialization_metrics(|| {
                self.execute_compiled_sql_context_with_phase_attribution::<E>(&compiled)
            });
        let (result, execute_cache_attribution, execute_phase_attribution) = executed?;
        let store_counters = store_counters_before.delta_since();
        let pure_covering_decode_local_instructions =
            current_pure_covering_decode_local_instructions()
                .saturating_sub(pure_covering_decode_before);
        let pure_covering_row_assembly_local_instructions =
            current_pure_covering_row_assembly_local_instructions()
                .saturating_sub(pure_covering_row_assembly_before);
        let attribution = sql_query_execution_attribution_from_inputs(
            &result,
            &SqlQueryExecutionAttributionInputs {
                compile_local_instructions,
                compile_phase_attribution,
                compile_cache_attribution,
                execute_cache_attribution,
                execute_phase_attribution,
                pure_covering_decode_local_instructions,
                pure_covering_row_assembly_local_instructions,
                projection_materialization,
                store_counters,
            },
        );

        Ok((result, attribution))
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_update::<E>(sql)?;

        self.execute_compiled_sql_owned::<E>(compiled)
    }

    /// Prepare one SQL DDL statement against the accepted schema catalog.
    ///
    /// This is a non-executing surface: it proves the statement can bind,
    /// derive an accepted-after snapshot, and pass schema mutation admission,
    /// then returns a prepared-only report without mutating schema or index
    /// storage.
    pub fn prepare_sql_ddl<E>(&self, sql: &str) -> Result<SqlDdlPreparationReport, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (_, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;

        Ok(prepared.report().clone())
    }

    fn prepare_sql_ddl_command<E>(
        &self,
        sql: &str,
    ) -> Result<(AcceptedSchemaCatalogContext, PreparedSqlDdlCommand), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (statement, _) =
            parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error)?;
        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let schema_info = catalog.accepted_schema_info_for::<E>();
        let prepared = match prepare_sql_ddl_statement(
            &statement,
            catalog.snapshot(),
            &schema_info,
            E::Store::PATH,
        ) {
            Ok(prepared) => prepared,
            Err(err) => return Err(QueryError::from_sql_ddl_prepare_error(err)),
        };

        Ok((catalog, prepared))
    }

    /// Execute one SQL DDL statement.
    ///
    /// Supported DDL routes through schema-owned physical work and
    /// accepted-snapshot publication.
    pub fn execute_sql_ddl<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (accepted_before, prepared) = self.prepare_sql_ddl_command::<E>(sql)?;
        if !prepared.mutates_schema() {
            return Ok(SqlStatementResult::Ddl(
                prepared
                    .report()
                    .clone()
                    .with_execution_status(SqlDdlExecutionStatus::NoOp),
            ));
        }

        let Some(derivation) = prepared.derivation() else {
            return Err(QueryError::unsupported_query());
        };
        let store = self
            .db
            .recovered_store(E::Store::PATH)
            .map_err(QueryError::execute)?;

        let (rows_scanned, index_keys_written) = Self::execute_prepared_sql_ddl_mutation::<E>(
            store,
            accepted_before.snapshot(),
            accepted_before.identity(),
            derivation,
            &prepared,
        )?;
        self.invalidate_accepted_schema_query_cache_for_entity::<E>();

        Ok(SqlStatementResult::Ddl(
            prepared
                .report()
                .clone()
                .with_execution_status(SqlDdlExecutionStatus::Published)
                .with_execution_metrics(rows_scanned, index_keys_written),
        ))
    }

    fn execute_prepared_sql_ddl_mutation<E>(
        store: crate::db::registry::StoreHandle,
        accepted_before: &AcceptedSchemaSnapshot,
        accepted_before_identity: crate::db::schema::AcceptedCatalogIdentity,
        derivation: &crate::db::schema::SchemaDdlAcceptedSnapshotDerivation,
        prepared: &PreparedSqlDdlCommand,
    ) -> Result<(usize, usize), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let metrics = match prepared.bound().statement() {
            crate::db::sql::ddl::BoundSqlDdlStatement::AddColumn(_) => {
                execute_sql_ddl_field_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::AlterColumnDefault(_) => {
                execute_sql_ddl_field_default_change(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::AlterColumnNullability(_) => {
                let rows_scanned = execute_sql_ddl_field_nullability_change(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (rows_scanned, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::DropColumn(_) => {
                execute_sql_ddl_field_drop(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::RenameColumn(_) => {
                execute_sql_ddl_field_rename(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::CreateIndex(create)
                if create.candidate_index().key().is_field_path_only() =>
            {
                execute_sql_ddl_field_path_index_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::CreateIndex(_) => {
                execute_sql_ddl_expression_index_addition(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::DropIndex(_) => {
                execute_sql_ddl_secondary_index_drop(
                    store,
                    E::ENTITY_TAG,
                    E::PATH,
                    accepted_before,
                    accepted_before_identity,
                    derivation,
                )
                .map_err(QueryError::from_sql_ddl_execution_error)?;

                (0, 0)
            }
            crate::db::sql::ddl::BoundSqlDdlStatement::NoOp(_) => (0, 0),
        };

        Ok(metrics)
    }
}
