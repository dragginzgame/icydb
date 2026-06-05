//! Module: db::session::query::paging::scalar
//! Responsibility: scalar load-query cursor paging.
//! Does not own: grouped cursor orchestration or fluent terminal adaptation.
//! Boundary: decodes scalar cursor tokens, delegates load execution, and finalizes scalar pages.

use crate::{
    db::{
        DbSession, PagedLoadExecutionWithTrace, PersistedRow, Query, QueryError,
        cursor::decode_optional_cursor_token,
        session::{finalize_scalar_paged_execution, query::query_error_from_executor_plan_error},
    },
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
}
