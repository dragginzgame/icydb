//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            execute_structural_aggregate_rows_for_canister,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                CompiledSqlCommand, SqlStatementResult,
                projection::sql_projection_statement_result_from_value_rows,
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
};

use super::aggregate_plan::PreparedAggregatePlanResolution;
use super::aggregate_request::PreparedAggregateRequestBundle;
use super::exact_aggregate::{ExactOutcome, ExactTarget};

impl<C: CanisterKind> DbSession<C> {
    fn execute_global_aggregate_with_prepared_plan(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Result<SqlStatementResult, QueryError> {
        let schema_info = catalog.accepted_schema_info();
        let bundle =
            PreparedAggregateRequestBundle::from_global_command(command, schema_info.clone())?;
        let (request, projection) = bundle.into_parts();
        let rows = execute_structural_aggregate_rows_for_canister(&self.db, prepared_plan, request)
            .map_err(QueryError::execute)?;
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let (columns, fixed_scales) = projection.into_components();

        sql_projection_statement_result_from_value_rows(
            catalog.enum_catalog(),
            columns,
            fixed_scales,
            rows,
            row_count,
        )
    }

    fn execute_global_aggregate_after_exact_target(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
        exact_target: ExactTarget,
        resolve_prepared_plan: impl FnOnce(Option<EntityAuthority>) -> PreparedAggregatePlanResolution,
    ) -> Result<SqlStatementResult, QueryError> {
        let exact_resolution = self.execute_exact_target(command, catalog, exact_target)?;
        let fallback_authority = match exact_resolution {
            ExactOutcome::Direct(result) => return Ok(result),
            ExactOutcome::Prepared(prepared_plan) => {
                return self.execute_global_aggregate_with_prepared_plan(
                    command,
                    catalog,
                    prepared_plan,
                );
            }
            ExactOutcome::Fallback { authority, .. } => authority,
        };

        let prepared_plan = resolve_prepared_plan(fallback_authority)?;

        self.execute_global_aggregate_with_prepared_plan(command, catalog, prepared_plan)
    }

    // Execute one borrowed compiled global aggregate while reusing its
    // compiled-command resident shared plan when the schema fingerprint still
    // matches the accepted snapshot carried by this execution context.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_catalog(
        &self,
        compiled: &CompiledSqlCommand,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let exact_target = self.resolve_compiled_exact_target(compiled, command, catalog)?;

        self.execute_global_aggregate_after_exact_target(
            command,
            catalog,
            exact_target,
            |fallback_authority| {
                self.resolve_compiled_global_aggregate_prepared_plan(
                    compiled,
                    command,
                    catalog,
                    fallback_authority,
                )
            },
        )
    }
}
