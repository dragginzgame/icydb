//! Module: db::session::query::grouped
//! Responsibility: accepted-schema-owned grouped query cursor orchestration.
//! Does not own: grouped planning, runtime aggregation, or response shaping.
//! Boundary: validates a shared prepared plan and delegates grouped execution.

use crate::{
    db::{
        DbSession, GroupedQueryOutput, QueryError,
        cursor::{ValidatedGroupedCursor, decode_optional_grouped_cursor_token},
        diagnostics::ExecutionTrace,
        executor::{
            ExecutionFamily, SharedPreparedExecutionPlan, StructuralGroupedProjectionResult,
            execute_shared_grouped_plan_for_canister,
        },
        query::{
            admission::{QueryAdmissionPolicy, QueryAdmissionSummary},
            intent::StructuralQuery,
        },
        session::{
            AcceptedSchemaCatalogContext, finalize_structural_grouped_projection_result,
            grouped_cursor_from_bytes, query::query_error_from_executor_plan_error,
        },
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::DiagnosticExecutionLane;

fn ensure_grouped_execution_family(family: ExecutionFamily) -> Result<(), QueryError> {
    match family {
        ExecutionFamily::Grouped => Ok(()),
        ExecutionFamily::PrimaryKey | ExecutionFamily::Ordered => Err(QueryError::invariant()),
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Plan, admit, and execute one engine-neutral grouped query.
    pub(in crate::db::session) fn execute_structural_grouped_from_query(
        &self,
        query: &StructuralQuery,
        catalog: &AcceptedSchemaCatalogContext,
        admission: Option<&QueryAdmissionPolicy>,
        cursor_token: Option<&str>,
    ) -> Result<GroupedQueryOutput, QueryError> {
        let execution_lane = if admission.is_some() {
            DiagnosticExecutionLane::PublicRead
        } else {
            DiagnosticExecutionLane::TrustedRead
        };
        let authority = catalog.accepted_entity_authority();
        let (prepared_plan, _) = self
            .cached_shared_query_plan_for_accepted_authority_with_catalog(
                authority,
                catalog,
                query,
                execution_lane,
            )?;
        if let Some(policy) = admission {
            let summary = policy.evaluate(QueryAdmissionSummary::from_plan(
                policy.lane(),
                prepared_plan.logical_plan(),
            ));
            if let Some(rejection) = summary.rejection() {
                return Err(QueryError::from(rejection.code()));
            }
        }

        let (result, _trace) = self.execute_structural_grouped_with_trace(
            prepared_plan,
            cursor_token,
            execution_lane,
        )?;
        let row_count = result.row_count();
        let (rows, next_cursor) = finalize_structural_grouped_projection_result(result)?;

        Ok(GroupedQueryOutput {
            entity: catalog.snapshot().entity_name().to_string(),
            rows,
            row_count,
            next_cursor: grouped_cursor_from_bytes(next_cursor),
        })
    }

    fn prepare_structural_grouped_execution(
        &self,
        plan: SharedPreparedExecutionPlan,
        cursor_token: Option<&str>,
    ) -> Result<(SharedPreparedExecutionPlan, ValidatedGroupedCursor), QueryError> {
        let authority = plan.authority_ref();
        self.ensure_accepted_schema_authority_is_current_for_store_path(
            authority.store_path(),
            plan.accepted_schema_authority()
                .map_err(QueryError::execute)?,
        )
        .map_err(QueryError::execute)?;
        ensure_grouped_execution_family(plan.execution_family().map_err(QueryError::execute)?)?;
        let cursor = decode_optional_grouped_cursor_token(cursor_token)
            .map_err(QueryError::from_cursor_plan_error)?;
        let cursor = plan
            .prepare_grouped_cursor_token(cursor)
            .map_err(query_error_from_executor_plan_error)?;

        Ok((plan, cursor))
    }

    /// Execute one accepted-schema-owned grouped plan without a generated type.
    pub(in crate::db::session) fn execute_structural_grouped_with_trace(
        &self,
        plan: SharedPreparedExecutionPlan,
        cursor_token: Option<&str>,
        execution_lane: DiagnosticExecutionLane,
    ) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), QueryError> {
        let (plan, cursor) = self.prepare_structural_grouped_execution(plan, cursor_token)?;

        execute_shared_grouped_plan_for_canister(&self.db, self.debug, plan, cursor, execution_lane)
            .map_err(QueryError::execute)
    }
}
