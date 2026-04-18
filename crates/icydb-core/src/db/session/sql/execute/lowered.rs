//! Module: db::session::sql::execute::lowered
//! Responsibility: bind lowered SQL commands onto structural query/aggregate
//! execution and preserve attribution or outward row-shape boundaries.
//! Does not own: lowered SQL parsing or public session API classification.
//! Boundary: keeps lowered-command execution bridges explicit and authority-aware.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{EntityAuthority, pipeline::execute_initial_grouped_rows_for_canister},
        query::intent::StructuralQuery,
        session::sql::{
            SqlCacheAttribution, SqlCompiledCommandCacheKey, SqlStatementResult,
            projection::{SqlProjectionPayload, grouped_sql_statement_result},
        },
        sql::lowering::{LoweredSelectShape, bind_lowered_sql_select_query_structural},
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Build one structural query from the lowered shared SQL SELECT shape so
    // both value-row and rendered-row statement surfaces reuse the same
    // lowered-to-structural binding boundary.
    fn structural_query_from_lowered_select(
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

    // Execute one lowered SQL SELECT through the shared lowered-to-structural
    // boundary and let the caller choose the final statement packaging.
    fn execute_lowered_sql_select_with<T>(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
        execute_structural: impl FnOnce(
            &Self,
            StructuralQuery,
            EntityAuthority,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let structural = Self::structural_query_from_lowered_select(select, authority)?;

        execute_structural(self, structural, authority)
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and keep the result in projection form.
    #[inline(never)]
    pub(in crate::db::session::sql::execute) fn execute_lowered_sql_projection_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        self.execute_lowered_sql_select_with(select, authority, |session, query, authority| {
            session
                .execute_structural_sql_projection_without_sql_cache(query, authority)
                .map(|(payload, _)| payload)
        })
    }

    // Execute one grouped SQL statement from one already prepared SQL select
    // cache entry so cached and uncached owners share the same runtime shell.
    fn execute_grouped_sql_statement_from_entry(
        &self,
        entry: crate::db::session::sql::SqlSelectPlanCacheEntry,
        authority: EntityAuthority,
    ) -> Result<SqlStatementResult, QueryError> {
        let (prepared_plan, columns, fixed_scales) = entry.into_parts();
        let plan = prepared_plan.logical_plan().clone();
        let page = execute_initial_grouped_rows_for_canister(&self.db, self.debug, authority, plan)
            .map_err(QueryError::execute)?;
        let next_cursor = page
            .next_cursor
            .map(|cursor| {
                let Some(token) = cursor.as_grouped() else {
                    return Err(QueryError::grouped_paged_emitted_scalar_continuation());
                };

                token.encode_hex().map_err(|err| {
                    QueryError::serialize_internal(format!(
                        "failed to serialize grouped continuation cursor: {err}"
                    ))
                })
            })
            .transpose()?;

        Ok(grouped_sql_statement_result(
            columns,
            fixed_scales,
            page.rows,
            next_cursor,
        ))
    }

    // Execute one normal compiled grouped SQL SELECT command through the
    // session-owned visibility-aware prepared-select owner.
    #[inline(never)]
    pub(in crate::db::session::sql::execute) fn execute_structural_sql_grouped_statement_select_core(
        &self,
        structural: StructuralQuery,
        authority: EntityAuthority,
        compiled_cache_key: &SqlCompiledCommandCacheKey,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError> {
        let (entry, cache_attribution) =
            self.planned_sql_select_with_visibility(&structural, authority, compiled_cache_key)?;

        Ok((
            self.execute_grouped_sql_statement_from_entry(entry, authority)?,
            cache_attribution,
        ))
    }
}
