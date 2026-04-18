//! Module: db::session::sql::execute::lowered
//! Responsibility: bind lowered SQL commands onto structural query/aggregate
//! execution and preserve attribution or outward row-shape boundaries.
//! Does not own: lowered SQL parsing or public session API classification.
//! Boundary: keeps lowered-command execution bridges explicit and authority-aware.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            pipeline::execute_initial_grouped_rows_for_canister,
        },
        query::intent::StructuralQuery,
        session::sql::{
            SqlCacheAttribution, SqlCompiledCommandCacheKey, SqlStatementResult,
            projection::{SqlProjectionPayload, grouped_sql_statement_result_from_page},
        },
        sql::lowering::{LoweredSelectShape, bind_lowered_sql_select_query_structural},
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Build one structural query from the lowered shared SQL SELECT shape so
    // parsed-SQL compile and lowered execution both reuse one canonical
    // lowered-to-structural binding boundary.
    pub(in crate::db::session::sql) fn structural_query_from_lowered_select(
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<crate::db::query::intent::StructuralQuery, QueryError> {
        bind_lowered_sql_select_query_structural(
            authority.model(),
            select,
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and keep the result in projection form.
    #[inline(never)]
    pub(in crate::db::session::sql::execute) fn execute_lowered_sql_projection_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        let query = Self::structural_query_from_lowered_select(select, authority)?;

        self.execute_structural_sql_projection_without_sql_cache(query, authority)
            .map(|(payload, _)| payload)
    }

    // Execute one grouped SQL statement from one shared lower prepared plan
    // plus one thin SQL projection contract so normal and diagnostics
    // surfaces share the same grouped plan-to-statement shell.
    pub(in crate::db::session::sql::execute) fn execute_grouped_sql_statement_from_prepared_plan_with<
        T,
    >(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: crate::db::session::sql::SqlProjectionContract,
        authority: EntityAuthority,
        execute_grouped: impl FnOnce(
            &Self,
            EntityAuthority,
            crate::db::query::plan::AccessPlannedQuery,
        )
            -> Result<(crate::db::executor::GroupedCursorPage, T), QueryError>,
    ) -> Result<(SqlStatementResult, T), QueryError> {
        let (columns, fixed_scales) = projection.into_parts();
        let plan = prepared_plan.logical_plan().clone();
        let (page, extra) = execute_grouped(self, authority, plan)?;

        Ok((
            grouped_sql_statement_result_from_page(columns, fixed_scales, page)?,
            extra,
        ))
    }

    // Execute one normal compiled grouped SQL SELECT command through the
    // shared lower query-plan cache plus the thin SQL projection contract.
    #[inline(never)]
    pub(in crate::db::session::sql::execute) fn execute_structural_sql_grouped_statement_select_core(
        &self,
        structural: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: &SqlCompiledCommandCacheKey,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let (prepared_plan, projection, cache_attribution) = self
            .sql_select_prepared_plan_with_compiled_cache(
                &structural,
                authority,
                compiled_cache_key.schema_fingerprint(),
            )?;

        let (statement_result, ()) = self.execute_grouped_sql_statement_from_prepared_plan_with(
            prepared_plan,
            projection,
            authority,
            |session, authority, plan| {
                execute_initial_grouped_rows_for_canister(
                    &session.db,
                    session.debug,
                    authority,
                    plan,
                )
                .map_err(QueryError::execute)
                .map(|page| (page, ()))
            },
        )?;

        Ok((statement_result, cache_attribution))
    }
}
