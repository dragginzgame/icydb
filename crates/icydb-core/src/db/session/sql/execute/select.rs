//! Module: db::session::sql::execute::select
//! Responsibility: SQL SELECT projection, grouped execution, and cache-aware
//! prepared-plan execution.
//! Does not own: SQL command routing, write execution, or EXPLAIN rendering.
//! Boundary: keeps SELECT plan-to-result adaptation out of the SQL execution hub.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan, StructuralGroupedProjectionResult,
            StructuralProjectionScanBudget,
        },
        query::intent::StructuralQuery,
        schema::AcceptedSchemaSnapshot,
        session::{
            finalize_structural_grouped_projection_result, grouped_cursor_from_bytes,
            query::{StructuralProjectionContract, StructuralProjectionPayload},
            sql::projection::{
                execute_sql_projection_rows_for_canister,
                execute_sql_projection_rows_for_canister_with_scan_budget,
                sql_statement_result_from_structural_projection_payload,
            },
            sql::{SqlCompiledCommandExecutionContext, SqlStatementResult},
        },
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

impl<C: CanisterKind> DbSession<C> {
    // Convert one grouped executor result plus SQL projection labels into the
    // statement result shape shared by normal and diagnostics SQL execution.
    pub(in crate::db::session::sql::execute) fn grouped_sql_statement_result_from_result(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        result: StructuralGroupedProjectionResult,
    ) -> Result<SqlStatementResult, QueryError> {
        let row_count = result.row_count();
        let (rows, continuation_cursor) = finalize_structural_grouped_projection_result(result)?;
        let next_cursor = grouped_cursor_from_bytes(continuation_cursor);

        Ok(SqlStatementResult::Grouped {
            columns,
            fixed_scales,
            rows,
            row_count,
            next_cursor,
        })
    }

    // Execute one SQL projection from one shared lower prepared plan plus
    // one shared structural projection contract so cached and explicit-bypass paths
    // share the same final row-materialization shell.
    fn execute_sql_projection_from_structural_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
        scan_budget: Option<StructuralProjectionScanBudget>,
    ) -> Result<StructuralProjectionPayload, QueryError> {
        let value_catalog = prepared_plan
            .authority_ref()
            .accepted_schema_info()
            .map(crate::db::schema::SchemaInfo::value_catalog_handle)
            .cloned()
            .ok_or_else(QueryError::invariant)?;
        let (columns, fixed_scales) = projection.into_components();
        let (rows, row_count) = match scan_budget {
            Some(scan_budget) => execute_sql_projection_rows_for_canister_with_scan_budget(
                &self.db,
                self.debug,
                prepared_plan,
                scan_budget,
            ),
            None => execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan),
        }
        .map_err(QueryError::execute)?;

        Ok(StructuralProjectionPayload::new(
            columns,
            fixed_scales,
            rows,
            row_count,
            value_catalog,
        ))
    }

    // Execute one SQL projection and immediately shape it into the public
    // statement-result envelope. Diagnostics keeps using the payload-returning
    // sibling so it can measure response finalization separately.
    fn execute_sql_statement_from_structural_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Result<SqlStatementResult, QueryError> {
        let payload = self.execute_sql_projection_from_structural_prepared_plan(
            prepared_plan,
            projection,
            None,
        )?;

        sql_statement_result_from_structural_projection_payload(payload)
    }

    // Execute one grouped SQL statement from one shared lowered prepared plan
    // plus one shared structural projection contract.
    fn execute_grouped_sql_statement_from_prepared_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
        execute_grouped: impl FnOnce(
            &Self,
            SharedPreparedExecutionPlan,
        ) -> Result<StructuralGroupedProjectionResult, QueryError>,
    ) -> Result<SqlStatementResult, QueryError> {
        let (columns, fixed_scales) = projection.into_components();
        let result = execute_grouped(self, prepared_plan)?;

        Self::grouped_sql_statement_result_from_result(columns, fixed_scales, result)
    }

    // Execute one SQL load query from a structural lowered query through the
    // shared lower query-plan cache while bypassing only the compiled SQL
    // command cache for lowered or aggregate-only paths.
    pub(in crate::db::session) fn execute_sql_projection_from_structural_query_without_sql_compiled_cache(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<StructuralProjectionPayload, QueryError> {
        let (prepared_plan, projection) = self.sql_select_prepared_plan_for_accepted_authority(
            &query,
            authority,
            accepted_schema,
        )?;

        self.execute_sql_projection_from_structural_prepared_plan(prepared_plan, projection, None)
    }

    // Execute one exact-mutation selector through a primary-only prepared plan
    // and one executor-enforced scanned-key ceiling.
    pub(in crate::db::session::sql) fn execute_primary_only_sql_projection_from_structural_query_with_scan_budget(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
        accepted_schema: &AcceptedSchemaSnapshot,
        scan_budget: StructuralProjectionScanBudget,
    ) -> Result<StructuralProjectionPayload, QueryError> {
        let (prepared_plan, projection) = self
            .sql_primary_only_select_prepared_plan_for_accepted_authority(
                &query,
                authority,
                accepted_schema,
            )?;

        self.execute_sql_projection_from_structural_prepared_plan(
            prepared_plan,
            projection,
            Some(scan_budget),
        )
    }

    pub(super) fn execute_select_compiled_sql_with_context(
        &self,
        query: &StructuralQuery,
        context: &SqlCompiledCommandExecutionContext,
    ) -> Result<SqlStatementResult, QueryError> {
        let resolved = self.resolve_select_prepared_plan_for_context(query, context)?;
        let (prepared_plan, projection) = resolved.into_parts();

        self.execute_select_compiled_sql_from_prepared_plan(query, prepared_plan, projection)
    }

    fn execute_select_compiled_sql_from_prepared_plan(
        &self,
        query: &StructuralQuery,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: StructuralProjectionContract,
    ) -> Result<SqlStatementResult, QueryError> {
        if query.has_grouping() {
            return self.execute_grouped_sql_statement_from_prepared_plan(
                prepared_plan,
                projection,
                |session, prepared_plan| {
                    session
                        .execute_structural_grouped_with_trace(
                            prepared_plan,
                            None,
                            DiagnosticExecutionLane::TrustedRead,
                        )
                        .map(|(result, _trace)| result)
                },
            );
        }

        self.execute_sql_statement_from_structural_prepared_plan(prepared_plan, projection)
    }
}
