//! Module: db::session::sql
//! Responsibility: session-owned SQL facade above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility and SQL subsystem exports in one index.

mod attribution;
mod cache;
mod compile;
mod compile_cache;
mod compiled;
mod ddl;
mod delete_policy;
mod execute;
mod projection;
mod result;
mod surface;
mod update_policy;
mod write_policy;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::StoreCounterSnapshot;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(test)]
use crate::db::sql::parser::{SqlStatement, parse_sql};
use crate::{
    db::{DbSession, PersistedRow, QueryError},
    traits::CanisterKind,
};

pub(in crate::db::session::sql) use crate::db::diagnostics::measure_local_instruction_delta as measure_sql_stage;
pub use crate::db::sql::ddl::{SqlDdlExecutionStatus, SqlDdlMutationKind, SqlDdlPreparationReport};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use attribution::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db::session::sql) use attribution::SqlQueryExecutionAttributionInputs;
#[cfg(feature = "diagnostics")]
pub use attribution::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution,
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
#[cfg(test)]
pub(in crate::db) const DEFAULT_PUBLIC_INSERT_STAGED_ROWS: u32 =
    write_policy::DEFAULT_PUBLIC_BOUNDED_WRITE_LIMIT;
pub use delete_policy::{
    SqlAdminBulkDeletePlan, SqlDeleteExposurePolicy, SqlDeletePolicyContext,
    SqlDeletePolicyRejection, SqlDeletePolicyReport, SqlDeleteStatementClassification,
    SqlPublicBoundedDeletePlan, SqlPublicPrimaryKeyDeletePlan, SqlSessionCurrentDeletePlan,
    SqlValidatedDeletePlan, classify_sql_delete_policy,
};
pub(in crate::db) use projection::SqlProjectionContract;
pub use result::SqlStatementResult;
pub use surface::{
    SqlStatementDispatch, SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
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

// Measure one SQL compile stage and immediately surface the stage result. The
// helper keeps attribution capture uniform while avoiding repeated
// `(cost, result); result?` boilerplate across the compile pipeline.
fn measured<T>(stage: impl FnOnce() -> Result<T, QueryError>) -> Result<(u64, T), QueryError> {
    let (local_instructions, result) = measure_sql_stage(stage);
    let value = result?;

    Ok((local_instructions, value))
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one trusted/admin single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities. It
    /// intentionally bypasses public-read admission, so its caller must own
    /// authorization and resource policy.
    pub fn execute_trusted_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let (compiled, _, _) = self.compile_sql_query_with_execution_context::<E>(sql)?;

        self.execute_compiled_sql_context_owned::<E>(compiled)
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C>,
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
        let attribution = SqlQueryExecutionAttribution::from_inputs(
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
        E: PersistedRow<Canister = C>,
    {
        let (compiled, _, _) = self.compile_sql_update_with_execution_context::<E>(sql)?;

        self.execute_compiled_sql_context_owned::<E>(compiled)
    }
}
