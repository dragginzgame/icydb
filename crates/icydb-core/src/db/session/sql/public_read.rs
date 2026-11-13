//! Module: db::session::sql::public_read
//! Responsibility: adapt caller-controlled SQL to existing public read contracts.
//! Does not own: authorization, admission policy, cursor encoding or SQL execution.
//! Boundary: compiled accepted-schema syntax -> bounded structural public reads.

use crate::{
    db::{
        DbSession, QueryError,
        query::admission::{GroupedAdmissionPolicy, QueryAdmissionPolicy, QueryAdmissionSummary},
        session::sql::{CompiledSqlCommand, SqlStatementDispatch, SqlStatementPageResult},
        sql::parser::{SqlExplainMode, SqlStatement},
    },
    error::InternalError,
    traits::CanisterKind,
    value::InputValue,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

impl<C: CanisterKind> DbSession<C> {
    /// Execute caller-controlled SQL through public read admission and paging.
    /// Authorization remains the caller's responsibility on every invocation.
    #[doc(hidden)]
    pub fn execute_sql_public_query_page_dispatch(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
        bindings: &[InputValue],
        continuation: Option<&str>,
    ) -> Result<SqlStatementPageResult, QueryError> {
        // Cursor-mode DISTINCT materializes/replays its source without the
        // scalar scan window. No complete public work/byte contract exists for
        // that terminal yet; trusted SQL retains the feature.
        if let SqlStatement::Select(select) = dispatch.statement()
            && select.distinct
        {
            return Err(QueryError::execute(InternalError::query_unsupported()));
        }
        // Execution EXPLAIN runs rows; its diagnostic lane cannot substitute
        // for public read admission, even when metadata introspection is allowed.
        if let SqlStatement::Explain(explain) = dispatch.statement()
            && matches!(
                explain.mode,
                SqlExplainMode::Execution | SqlExplainMode::ExecutionJson
            )
        {
            return Err(QueryError::execute(InternalError::query_unsupported()));
        }
        let (context, entity) =
            self.compile_sql_query_with_dispatch_execution_context(dispatch, bindings)?;
        self.ensure_sql_query_execution_context_is_current(&context)?;
        let (command, catalog) = context.into_command_and_catalog();
        match command {
            CompiledSqlCommand::Select { query } if !query.has_grouping() => {
                let (page, fixed_scales) =
                    self.execute_public_structural_live_page(query, &catalog, continuation)?;
                Ok(SqlStatementPageResult::Scalar { page, fixed_scales })
            }
            CompiledSqlCommand::Select { query } => {
                // Reuse the canonical public grouped caps, not SQL's trusted
                // defaults. The existing grouped terminal owns its continuation.
                let grouped = GroupedAdmissionPolicy::default_bounded_read();
                let groups = grouped
                    .max_groups()
                    .ok_or_else(QueryError::invariant)?
                    .get();
                let bytes = grouped
                    .max_group_bytes()
                    .ok_or_else(QueryError::invariant)?
                    .get();
                let limit = query
                    .scalar_load_window()
                    .ok_or_else(QueryError::invariant)?
                    .limit()
                    .unwrap_or(groups)
                    .min(groups);
                let query = query
                    .grouped_limits(u64::from(groups), u64::from(bytes))
                    .limit(limit);
                let lane = DiagnosticExecutionLane::PublicRead;
                let (plan, projection) = self
                    .structural_projection_prepared_plan_for_accepted_authority(
                        &query,
                        catalog.accepted_entity_authority(),
                        catalog.snapshot(),
                        lane,
                    )?;
                let policy = QueryAdmissionPolicy::default_bounded_read();
                let summary = policy.evaluate(
                    QueryAdmissionSummary::from_plan(policy.lane(), plan.logical_plan())
                        .map_err(QueryError::execute)?,
                );
                if let Some(rejection) = summary.rejection() {
                    return Err(QueryError::from(rejection.code()));
                }
                let result = self.execute_grouped_sql_statement_from_prepared_plan(
                    plan,
                    projection,
                    |session, plan| session.execute_structural_grouped(plan, continuation, lane),
                )?;
                Ok(SqlStatementPageResult::Statement { result, entity })
            }
            // Global SQL aggregates have no bounded public terminal yet. Never
            // route them through their trusted collect-all/exact-target shortcut.
            CompiledSqlCommand::GlobalAggregate { .. } => {
                Err(QueryError::execute(InternalError::query_unsupported()))
            }
            command => {
                if continuation.is_some() {
                    return Err(QueryError::execute(InternalError::query_unsupported()));
                }
                let result = self.execute_compiled_sql_query_with_catalog(&command, &catalog)?;
                Ok(SqlStatementPageResult::Statement { result, entity })
            }
        }
    }
}
