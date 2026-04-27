//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate adapter wiring and response shaping.
//! Does not own: aggregate terminal construction, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{EntityAuthority, StructuralAggregateRequest},
        session::sql::{
            SqlCacheAttribution, SqlStatementResult,
            projection::{
                SqlProjectionPayload, projection_fixed_scales_from_projection_spec,
                projection_labels_from_projection_spec,
            },
        },
        sql::lowering::SqlGlobalAggregateCommandCore,
    },
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Execute one prepared SQL aggregate command through executor-owned
    // structural aggregate execution, then shape the completed rows into the
    // SQL projection payload.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement_for_authority<
        E,
    >(
        &self,
        command: SqlGlobalAggregateCommandCore,
        _authority: EntityAuthority,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (query, strategies, projection, having) = command.into_execution_parts();
        let columns = projection_labels_from_projection_spec(&projection);
        let fixed_scales = projection_fixed_scales_from_projection_spec(&projection);
        let terminals = strategies
            .into_iter()
            .map(|strategy| {
                strategy
                    .into_executor_terminal()
                    .map_err(QueryError::invariant)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = StructuralAggregateRequest::new(terminals, projection, having);
        let query = Query::<E>::from_inner(query);
        let (prepared_plan, cache_attribution) =
            self.cached_shared_query_plan_for_entity::<E>(&query)?;
        let result = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_structural_aggregate_result(&prepared_plan, request)
            })
            .map_err(QueryError::execute)?;
        let rows = result.into_value_rows();
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok((
            SqlProjectionPayload::new(columns, fixed_scales, rows, row_count)
                .into_statement_result(),
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
    }
}
