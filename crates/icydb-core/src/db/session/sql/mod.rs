//! Module: db::session::sql
//! Responsibility: session-owned SQL facade above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility and SQL subsystem exports in one index.

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

use crate::{
    db::{DbSession, QueryError},
    traits::CanisterKind,
};

pub use crate::db::sql::ddl::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport,
};
pub(in crate::db) use cache::SqlCompiledCommandCacheKey;
pub(in crate::db::session::sql) use cache::{
    SqlCompiledCommandCacheContext, SqlCompiledCommandSurface,
};
pub(in crate::db) use compiled::{
    CompiledSqlCommand, CompiledSqlInsertCommand, SqlCompiledCommandExecutionContext,
    SqlCompiledSchemaFingerprint, SqlGlobalAggregateCachedPlan, SqlGlobalAggregatePlanCacheEntry,
};
pub(in crate::db) use delete_policy::{
    SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
    SqlPublicPrimaryKeyDeletePlan, SqlValidatedDeletePlan, classify_sql_delete_statement_policy,
};
pub use integrity::SqlIntegrityError;
pub use result::SqlStatementResult;
pub(in crate::db::session) use resumable_update::validate_current_initial_mutation_job_continuation;
pub(in crate::db::session::sql) use surface::sql_statement_entity_name_from_statement;
pub use surface::{
    SqlStatementDispatch, SqlStatementShellSurface, SqlStatementSurface, sql_statement_dispatch,
    sql_statement_shell_surface, sql_statement_surface,
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

impl<C: CanisterKind> DbSession<C> {
    /// Execute one trusted/admin single-entity reduced SQL query or introspection statement.
    ///
    /// The statement resolves its entity against accepted catalog authority,
    /// rejects state-changing SQL, and returns SQL-shaped output. It
    /// intentionally bypasses public-read admission. The request root still
    /// supplies finite physical and aggregate execution policy; its caller
    /// separately owns authorization.
    pub fn execute_trusted_sql_query(&self, sql: &str) -> Result<SqlStatementResult, QueryError> {
        let dispatch = sql_statement_dispatch(sql)?;
        self.execute_trusted_sql_query_with_entity_name(&dispatch)
            .map(|(result, _)| result)
    }

    /// Execute one trusted query from an admitted parsed dispatch.
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_with_entity_name(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<(SqlStatementResult, String), QueryError> {
        let (compiled, entity_name) =
            self.compile_sql_query_with_dispatch_execution_context(dispatch)?;
        let result = self.execute_compiled_sql_query_context_owned(compiled)?;

        Ok((result, entity_name))
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
        let dispatch = sql_statement_dispatch(sql)?;

        self.execute_trusted_sql_mutation_dispatch(&dispatch)
    }

    /// Execute one trusted mutation from its admitted parsed dispatch artifact.
    #[doc(hidden)]
    pub fn execute_trusted_sql_mutation_dispatch(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<SqlStatementResult, QueryError> {
        let compiled = self.compile_sql_mutation_with_execution_context(dispatch)?;

        self.execute_compiled_sql_context_owned(compiled)
    }
}
