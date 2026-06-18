//! Module: db::session::query::paging::grouped
//! Responsibility: grouped query cursor orchestration.
//! Does not own: scalar paging or grouped runtime execution semantics.
//! Boundary: decodes grouped cursor tokens, delegates grouped execution, and finalizes grouped pages.

#[cfg(feature = "diagnostics")]
use crate::db::executor::GroupedExecutePhaseAttribution;
use crate::{
    db::{
        DbSession, PagedGroupedExecutionWithTrace, PersistedRow, Query, QueryError,
        cursor::{ValidatedGroupedCursor, decode_optional_grouped_cursor_token},
        diagnostics::ExecutionTrace,
        executor::{LoadExecutor, PreparedExecutionPlan, StructuralGroupedProjectionResult},
        session::{
            finalize_structural_grouped_projection_result,
            query::query_error_from_executor_plan_error,
        },
    },
    error::InternalError,
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    /// Execute one grouped query page with optional grouped continuation cursor.
    ///
    /// This is the explicit grouped execution boundary; scalar load APIs reject
    /// grouped plans to preserve scalar response contracts.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "crate-local grouped pagination tests exercise this boundary before public grouped paging APIs expose it"
        )
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
            ValidatedGroupedCursor,
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

    // Execute one grouped prepared plan result with optional grouped cursor
    // while preserving executor phase attribution for diagnostics surfaces.
    #[cfg(feature = "diagnostics")]
    pub(in crate::db::session) fn execute_grouped_with_phase_attribution<E>(
        &self,
        plan: PreparedExecutionPlan<E>,
        cursor_token: Option<&str>,
    ) -> Result<
        (
            StructuralGroupedProjectionResult,
            Option<ExecutionTrace>,
            GroupedExecutePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_grouped_with_cursor(plan, cursor_token, |executor, plan, cursor| {
            executor.execute_grouped_paged_with_cursor_traced_with_phase_attribution(plan, cursor)
        })
    }
}
