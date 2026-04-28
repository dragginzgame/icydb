//! Module: db::session::query::paging
//! Responsibility: scalar paging and grouped query cursor orchestration.
//! Does not own: fluent terminal adaptation, explain rendering, or diagnostics attribution.
//! Boundary: decodes external cursor tokens, delegates execution, and finalizes session paging results.

use crate::{
    db::{
        DbSession, PagedGroupedExecutionWithTrace, PagedLoadExecutionWithTrace, PersistedRow,
        Query, QueryError,
        cursor::{
            GroupedPlannedCursor, decode_optional_cursor_token,
            decode_optional_grouped_cursor_token,
        },
        diagnostics::ExecutionTrace,
        executor::{LoadExecutor, PreparedExecutionPlan, StructuralGroupedProjectionResult},
        session::{
            finalize_scalar_paged_execution, finalize_structural_grouped_projection_result,
            query::query_error_from_executor_plan_error,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one scalar paged load query and return optional continuation cursor plus trace.
    pub(crate) fn execute_load_query_paged_with_trace<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedLoadExecutionWithTrace<E>, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build/validate prepared execution plan and reject grouped plans.
        let plan = self.cached_prepared_query_plan_for_entity::<E>(query)?.0;
        Self::ensure_scalar_paged_execution_family(
            plan.execution_family().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external cursor token and validate it against plan surface.
        let cursor_bytes = decode_optional_cursor_token(cursor_token)
            .map_err(QueryError::from_cursor_plan_error)?;
        let cursor = plan
            .prepare_cursor(cursor_bytes.as_deref())
            .map_err(query_error_from_executor_plan_error)?;

        // Phase 3: execute one traced page and encode outbound continuation token.
        let (page, trace) = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_paged_with_cursor_traced(plan, cursor)
            })
            .map_err(QueryError::execute)?;
        finalize_scalar_paged_execution(page, trace)
    }

    /// Execute one grouped query page with optional grouped continuation cursor.
    ///
    /// This is the explicit grouped execution boundary; scalar load APIs reject
    /// grouped plans to preserve scalar response contracts.
    #[allow(
        dead_code,
        reason = "cursor-aware grouped execution remains a session boundary used by tests and adjacent SQL paths"
    )]
    pub(in crate::db) fn execute_grouped<E>(
        &self,
        query: &Query<E>,
        cursor_token: Option<&str>,
    ) -> Result<PagedGroupedExecutionWithTrace, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: build the prepared execution plan once from the typed query.
        let plan = self.cached_prepared_query_plan_for_entity::<E>(query)?.0;

        // Phase 2: reuse the shared prepared grouped execution path and then
        // finalize the outward grouped payload at the session boundary.
        let (result, trace) = self.execute_grouped_with_trace(plan, cursor_token)?;

        finalize_structural_grouped_projection_result(result, trace)
    }

    // Execute one grouped prepared plan page with optional grouped cursor
    // while letting the caller choose the final grouped-runtime dispatch.
    pub(in crate::db::session) fn execute_grouped_with_cursor<E, T>(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor_token: Option<&str>,
        op: impl FnOnce(
            LoadExecutor<E>,
            PreparedExecutionPlan<E>,
            GroupedPlannedCursor,
        ) -> Result<T, InternalError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: validate the prepared plan shape before decoding cursors.
        Self::ensure_grouped_execution_family(
            plan.execution_family().map_err(QueryError::execute)?,
        )?;

        // Phase 2: decode external grouped cursor token and validate against plan.
        let cursor = decode_optional_grouped_cursor_token(cursor_token)
            .map_err(QueryError::from_cursor_plan_error)?;
        let cursor = plan
            .prepare_grouped_cursor_token(cursor)
            .map_err(query_error_from_executor_plan_error)?;

        // Phase 3: execute one grouped page while preserving the structural
        // grouped cursor payload for whichever outward cursor format the caller needs.
        self.with_metrics(|| op(self.load_executor::<E>(), plan, cursor))
            .map_err(QueryError::execute)
    }

    // Execute one grouped prepared plan result with optional grouped cursor.
    pub(in crate::db::session) fn execute_grouped_with_trace<E>(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor_token: Option<&str>,
    ) -> Result<(StructuralGroupedProjectionResult, Option<ExecutionTrace>), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_grouped_with_cursor(plan, cursor_token, |executor, plan, cursor| {
            executor.execute_grouped_paged_with_cursor_traced(plan, cursor)
        })
    }
}
