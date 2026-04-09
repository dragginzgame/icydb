//! Module: db::session::sql::dispatch::lowered
//! Responsibility: module-local ownership and contracts for db::session::sql::dispatch::lowered.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{EntityAuthority, execute_sql_delete_projection_for_canister},
        session::sql::{
            SqlDispatchResult,
            projection::{
                SqlProjectionPayload, projection_labels_from_entity_model,
                sql_projection_rows_from_kernel_rows,
            },
            surface::{SqlSurface, session_sql_lane, unsupported_sql_lane_message},
        },
        sql::lowering::{
            LoweredBaseQueryShape, LoweredSelectShape, LoweredSqlCommand, LoweredSqlQuery,
            bind_lowered_sql_delete_query_structural, bind_lowered_sql_select_query_structural,
        },
    },
    traits::CanisterKind,
    value::Value,
};

type SqlQuerySurfaceRowParts = (Vec<String>, Vec<Vec<Value>>, u32);

impl<C: CanisterKind> DbSession<C> {
    // Build one structural query from the lowered shared SQL SELECT shape so
    // both value-row and rendered-row dispatch surfaces reuse the same
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

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and keep the result in projection form.
    #[inline(never)]
    fn execute_lowered_sql_projection_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        let structural = Self::structural_query_from_lowered_select(select, authority)?;

        self.execute_structural_sql_projection(structural, authority)
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and package it for the shared core dispatch
    // lane using canonical value rows.
    #[inline(never)]
    pub(in crate::db::session::sql::dispatch) fn execute_lowered_sql_dispatch_select_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        self.execute_lowered_sql_projection_core(select, authority)
            .map(SqlProjectionPayload::into_dispatch_result)
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and package it for the generated query
    // surface when the terminal short path can prove rendered SQL rows
    // directly.
    #[inline(never)]
    fn execute_lowered_sql_dispatch_select_text_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let structural = Self::structural_query_from_lowered_select(select, authority)?;

        self.execute_structural_sql_projection_text(structural, authority)
    }

    // Execute one lowered SQL DELETE command through the shared structural
    // delete projection path.
    fn execute_lowered_sql_dispatch_delete_core(
        &self,
        delete: &LoweredBaseQueryShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let structural = bind_lowered_sql_delete_query_structural(
            authority.model(),
            delete.clone(),
            MissingRowPolicy::Ignore,
        );
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let deleted = execute_sql_delete_projection_for_canister(
            &self.db,
            authority,
            structural.build_plan_with_visible_indexes(&visible_indexes)?,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows);

        Ok(SqlProjectionPayload::new(
            projection_labels_from_entity_model(authority.model()),
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    /// Execute one already-lowered shared SQL query shape for resolved authority.
    #[doc(hidden)]
    pub fn execute_lowered_sql_dispatch_query_for_authority(
        &self,
        lowered: LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        self.execute_lowered_sql_dispatch_query_text_core(lowered, authority)
    }

    /// Execute one already-lowered shared SQL `SELECT` shape for resolved authority.
    ///
    /// This narrower boundary exists specifically for generated canister query
    /// surfaces that need row-shaped SQL payloads without retaining the full
    /// typed dispatch enum in the outer query facade.
    #[doc(hidden)]
    pub fn execute_lowered_sql_projection_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlQuerySurfaceRowParts, QueryError> {
        let Some(query) = lowered.query() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                session_sql_lane(lowered),
            )));
        };

        match query {
            LoweredSqlQuery::Select(select) => self
                .execute_lowered_sql_projection_core(select.clone(), authority)
                .map(SqlProjectionPayload::into_parts),
            LoweredSqlQuery::Delete(delete) => self
                .execute_lowered_sql_dispatch_delete_core(delete, authority)
                .and_then(|dispatch| match dispatch {
                    SqlDispatchResult::Projection {
                        columns,
                        rows,
                        row_count,
                    } => Ok((columns, rows, row_count)),
                    SqlDispatchResult::ProjectionText { .. }
                    | SqlDispatchResult::Explain(_)
                    | SqlDispatchResult::Describe(_)
                    | SqlDispatchResult::ShowIndexes(_)
                    | SqlDispatchResult::ShowColumns(_)
                    | SqlDispatchResult::ShowEntities(_) => Err(QueryError::unsupported_query(
                        "generated SQL query dispatch requires row-shaped SELECT or DELETE",
                    )),
                }),
        }
    }

    // Execute one lowered SQL query command for the generated query surface,
    // which may keep rendered SQL projection rows when the terminal short path
    // can prove them directly.
    fn execute_lowered_sql_dispatch_query_text_core(
        &self,
        lowered: LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        let lane = session_sql_lane(&lowered);
        let Some(query) = lowered.into_query() else {
            return Err(QueryError::unsupported_query(unsupported_sql_lane_message(
                SqlSurface::QueryFrom,
                lane,
            )));
        };

        match query {
            LoweredSqlQuery::Select(select) => {
                self.execute_lowered_sql_dispatch_select_text_core(select, authority)
            }
            LoweredSqlQuery::Delete(delete) => {
                self.execute_lowered_sql_dispatch_delete_core(&delete, authority)
            }
        }
    }
}
