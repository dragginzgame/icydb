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
mod integrity;
mod projection;
mod result;
mod resumable_update;
mod surface;
mod update_policy;
mod write_policy;

#[cfg(feature = "diagnostics")]
use crate::db::diagnostics::{
    StoreCounterSnapshot, begin_sql_structural_work_attribution,
    finish_sql_structural_work_attribution,
};
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
use crate::{
    db::{DbSession, QueryError},
    traits::CanisterKind,
};

pub(in crate::db::session::sql) use crate::db::diagnostics::measure_local_instruction_delta as measure_sql_stage;
pub use crate::db::sql::ddl::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport,
};
#[cfg(feature = "diagnostics")]
pub(in crate::db) use attribution::SqlExecutePhaseAttribution;
#[cfg(feature = "diagnostics")]
pub(in crate::db::session::sql) use attribution::SqlQueryExecutionAttributionInputs;
#[cfg(feature = "diagnostics")]
pub use attribution::{
    SqlCompileAttribution, SqlDistinctProjectionAttribution, SqlExecutionAttribution,
    SqlHybridCoveringAttribution, SqlOutputBlobAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
};
pub(in crate::db) use cache::{SqlCacheAttribution, SqlCompiledCommandCacheKey};
pub(in crate::db::session::sql) use cache::{
    SqlCompiledCommandCacheContext, SqlCompiledCommandSurface,
    sql_compiled_command_cache_miss_reason,
};
pub(in crate::db::session::sql) use compile::{
    SqlCompileAttributionBuilder, SqlCompilePhaseAttribution,
};
pub(in crate::db) use compiled::{
    CompiledSqlCommand, CompiledSqlInsertCommand, SqlCompiledCommandExecutionContext,
    SqlCompiledSchemaFingerprint, SqlGlobalAggregateCountPlanCacheEntry,
};
pub(in crate::db) use delete_policy::{
    SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
    SqlPublicPrimaryKeyDeletePlan, SqlValidatedDeletePlan, classify_sql_delete_policy,
};
pub use integrity::SqlIntegrityError;
pub use result::SqlStatementResult;
pub use surface::{
    SqlStatementDispatch, SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_entity_name, sql_statement_shell_surface, sql_statement_surface,
};
pub(in crate::db) use update_policy::{
    SqlExactUpdatePolicy, SqlExactUpdatePolicyRejection, SqlPublicBoundedUpdatePlan,
    SqlPublicPrimaryKeyUpdatePlan, SqlResumableUpdatePolicyReport, SqlTrustedExactUpdatePlan,
    SqlTrustedResumableUpdatePlan, SqlUpdateExposurePolicy, SqlUpdatePolicyRejection,
    SqlUpdatePolicyReport, SqlValidatedUpdatePlan, classify_sql_resumable_update_policy,
    classify_sql_update_policy_for_entity, with_accepted_sql_update_policy_context,
};
pub(in crate::db::session::sql) use write_policy::combined_optional_row_bound;
#[cfg(test)]
pub(in crate::db) use write_policy::{SqlWriteExecutionBounds, SqlWriteReturningBounds};

#[cfg(feature = "diagnostics")]
use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;

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
    /// The statement resolves its entity against accepted catalog authority,
    /// rejects state-changing SQL, and returns SQL-shaped output. It
    /// intentionally bypasses public-read admission. The request root still
    /// supplies finite physical and aggregate execution policy; its caller
    /// separately owns authorization.
    pub fn execute_trusted_sql_query(&self, sql: &str) -> Result<SqlStatementResult, QueryError> {
        self.execute_trusted_sql_query_with_entity_name(sql)
            .map(|(result, _)| result)
    }

    /// Execute one trusted query and return the entity name resolved by the
    /// same canonical parse used for compilation.
    ///
    /// This hidden facade seam prevents callers that need the response entity
    /// from parsing the complete statement a second time.
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_entity_name(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, String), QueryError> {
        let entity_name = sql_statement_entity_name(sql)?;
        let (compiled, _, _) =
            self.compile_sql_query_with_execution_context(entity_name.as_deref(), sql)?;
        let result = self.execute_compiled_sql_query_context_owned(compiled)?;

        Ok((result, entity_name.unwrap_or_default()))
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_attribution(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError> {
        begin_sql_structural_work_attribution();
        let entity_name = sql_statement_entity_name(sql)?;
        // Phase 1: measure the compile side of the new seam, including parse,
        // surface validation, and semantic command construction.
        let (compile_local_instructions, compiled) = measure_sql_stage(|| {
            self.compile_sql_query_with_execution_context(entity_name.as_deref(), sql)
        });
        let (compiled, compile_cache_attribution, compile_phase_attribution) = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let store_counters_before = StoreCounterSnapshot::capture();
        let pure_covering_decode_before = current_pure_covering_decode_local_instructions();
        let pure_covering_row_assembly_before =
            current_pure_covering_row_assembly_local_instructions();
        let (executed, projection_materialization) =
            with_sql_projection_materialization_metrics(|| {
                self.execute_compiled_sql_query_context_with_phase_attribution(&compiled)
            });
        let (result, execute_cache_attribution, execute_phase_attribution) = executed?;
        let structural_work = finish_sql_structural_work_attribution();
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
                structural_work,
                store_counters,
            },
        );

        Ok((result, attribution))
    }

    /// Execute one trusted single-entity SQL `INSERT` or `DELETE` statement.
    ///
    /// This surface stays hard-bound to `E` and rejects reads and `UPDATE`.
    /// Trusted updates must choose the exact complete-set or intentional
    /// ordered-prefix contract explicitly.
    pub fn execute_trusted_sql_mutation(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError> {
        let (compiled, _, _) = self.compile_sql_mutation_with_execution_context(sql)?;

        self.execute_compiled_sql_context_owned(compiled)
    }
}
