//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{SharedPreparedExecutionPlan, execute_structural_aggregate_rows_for_canister},
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlStatementResult, projection::sql_projection_statement_result_from_value_rows,
            },
        },
        sql::lowering::SqlGlobalAggregateCommand,
    },
    traits::CanisterKind,
};

use super::aggregate_plan::PreparedAggregatePlanResolution;
use super::aggregate_request::PreparedAggregateRequestBundle;
use super::exact_aggregate::ExactTarget;

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
        resolve_prepared_plan: impl FnOnce() -> PreparedAggregatePlanResolution,
    ) -> Result<SqlStatementResult, QueryError> {
        if let Some(result) = self.execute_exact_target(command, catalog, exact_target)? {
            return Ok(result);
        }

        let prepared_plan = resolve_prepared_plan()?;

        self.execute_global_aggregate_with_prepared_plan(command, catalog, prepared_plan)
    }

    // Resolve exact metadata against accepted authority; ordinary preparation
    // is retained only by the shared weighted query-plan cache.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_compiled_statement_ref_with_catalog(
        &self,
        command: &SqlGlobalAggregateCommand,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let exact_target = self.resolve_compiled_exact_target(command, catalog)?;

        self.execute_global_aggregate_after_exact_target(command, catalog, exact_target, || {
            self.resolve_compiled_global_aggregate_prepared_plan(command, catalog)
        })
    }
}
