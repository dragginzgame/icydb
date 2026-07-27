//! Module: db::session::query::grouped
//! Responsibility: accepted-schema-owned grouped query cursor orchestration.
//! Does not own: grouped planning, runtime aggregation, or response shaping.
//! Boundary: validates a shared prepared plan and delegates grouped execution.

#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    GroupedExecutePhaseAttribution, execute_shared_grouped_plan_for_canister_with_phase_attribution,
};
use crate::{
    db::{
        DbSession, QueryError,
        cursor::decode_optional_grouped_cursor_token,
        diagnostics::ExecutionTrace,
        executor::{
            ExecutionFamily, SharedPreparedExecutionPlan, StructuralGroupedProjectionResult,
            execute_shared_grouped_plan_for_canister,
        },
        session::query::query_error_from_executor_plan_error,
    },
    traits::CanisterKind,
};

fn ensure_grouped_execution_family(family: ExecutionFamily) -> Result<(), QueryError> {
    match family {
        ExecutionFamily::Grouped => Ok(()),
        ExecutionFamily::PrimaryKey | ExecutionFamily::Ordered => Err(QueryError::invariant()),
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one accepted-schema-owned grouped plan without a generated type.
    pub(in crate::db::session) fn execute_structural_grouped_with_trace(
        &self,
        plan: SharedPreparedExecutionPlan,
        cursor_token: Option<&str>,
    ) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), QueryError> {
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

        self.with_metrics(|| {
            execute_shared_grouped_plan_for_canister(&self.db, self.debug, plan, cursor)
        })
        .map_err(QueryError::execute)
    }

    /// Execute one accepted-schema-owned grouped plan with phase attribution.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db::session) fn execute_structural_grouped_with_phase_attribution(
        &self,
        plan: SharedPreparedExecutionPlan,
        cursor_token: Option<&str>,
    ) -> Result<
        (
            StructuralGroupedProjectionResult,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        QueryError,
    > {
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

        self.with_metrics(|| {
            execute_shared_grouped_plan_for_canister_with_phase_attribution(
                &self.db, self.debug, plan, cursor,
            )
        })
        .map_err(QueryError::execute)
    }
}
