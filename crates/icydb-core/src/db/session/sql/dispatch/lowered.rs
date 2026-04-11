//! Module: db::session::sql::dispatch::lowered
//! Responsibility: bind lowered SQL commands onto structural query/aggregate
//! execution and preserve attribution or outward row-shape boundaries.
//! Does not own: lowered SQL parsing or public session API classification.
//! Boundary: keeps lowered-command execution bridges explicit and authority-aware.

#[cfg(feature = "perf-attribution")]
use crate::db::{
    executor::attribute_sql_projection_text_rows_for_canister,
    session::sql::{
        SqlProjectionTextExecutorAttribution, projection::projection_labels_from_projection_spec,
    },
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{
            EntityAuthority, execute_initial_grouped_rows_for_canister,
            execute_sql_delete_projection_for_canister,
        },
        query::intent::StructuralQuery,
        session::sql::{
            SqlDispatchResult,
            projection::{
                SqlProjectionPayload, projection_labels_from_fields,
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

///
/// LoweredSqlDispatchExecutorAttribution
///
/// LoweredSqlDispatchExecutorAttribution breaks the lowered SQL dispatch
/// executor path into structural bind, visible-index lookup, plan build,
/// projection-label derivation, executor internals, and final dispatch result
/// packaging.
/// This keeps perf attribution attached to the stable lowered SQL boundary
/// instead of scattering measurement logic across unrelated callers.
///

#[cfg(feature = "perf-attribution")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoweredSqlDispatchExecutorAttribution {
    pub bind_local_instructions: u64,
    pub visible_indexes_local_instructions: u64,
    pub build_plan_local_instructions: u64,
    pub projection_labels_local_instructions: u64,
    pub projection_executor: SqlProjectionTextExecutorAttribution,
    pub dispatch_result_local_instructions: u64,
    pub total_local_instructions: u64,
}

#[cfg(feature = "perf-attribution")]
const fn read_local_instruction_counter() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        canic_cdk::api::performance_counter(1)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        0
    }
}

#[cfg(feature = "perf-attribution")]
fn measure_dispatch_result<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

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

    // Execute one lowered SQL SELECT through the shared lowered-to-structural
    // boundary and let the caller choose the final dispatch packaging.
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
    pub(in crate::db::session::sql::dispatch) fn execute_lowered_sql_projection_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        self.execute_lowered_sql_select_with(
            select,
            authority,
            Self::execute_structural_sql_projection,
        )
    }

    // Execute one lowered SQL SELECT command entirely through the shared
    // structural projection path and package it for the generated query
    // surface when the terminal short path can prove rendered SQL rows
    // directly.
    #[inline(never)]
    pub(in crate::db::session::sql::dispatch) fn execute_lowered_sql_dispatch_select_text_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        self.execute_lowered_sql_select_with(
            select,
            authority,
            Self::execute_structural_sql_projection_text,
        )
    }

    #[cfg(feature = "perf-attribution")]
    #[doc(hidden)]
    pub fn attribute_lowered_sql_dispatch_query_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<LoweredSqlDispatchExecutorAttribution, QueryError> {
        let Some(LoweredSqlQuery::Select(select)) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "executor attribution currently supports lowered SQL SELECT only",
            ));
        };

        let (bind_local_instructions, structural) = measure_dispatch_result(|| {
            Self::structural_query_from_lowered_select(select, authority)
        });
        let structural = structural?;

        let (visible_indexes_local_instructions, visible_indexes) = measure_dispatch_result(|| {
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())
        });
        let visible_indexes = visible_indexes?;

        let (build_plan_local_instructions, plan) = measure_dispatch_result(|| {
            structural.build_plan_with_visible_indexes(&visible_indexes)
        });
        let plan = plan?;

        let (projection_labels_local_instructions, columns) = measure_dispatch_result(|| {
            let projection = plan.projection_spec(authority.model());

            Ok::<Vec<String>, QueryError>(projection_labels_from_projection_spec(&projection))
        });
        let columns = columns?;

        let (projection_executor, projected) =
            attribute_sql_projection_text_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;

        let (dispatch_result_local_instructions, dispatch_result) = measure_dispatch_result(|| {
            let (rows, row_count) = projected.into_parts();

            Ok::<SqlDispatchResult, QueryError>(SqlDispatchResult::ProjectionText {
                columns,
                rows,
                row_count,
            })
        });
        let _dispatch_result = dispatch_result?;

        let total_local_instructions = bind_local_instructions
            .saturating_add(visible_indexes_local_instructions)
            .saturating_add(build_plan_local_instructions)
            .saturating_add(projection_labels_local_instructions)
            .saturating_add(projection_executor.total)
            .saturating_add(dispatch_result_local_instructions);

        Ok(LoweredSqlDispatchExecutorAttribution {
            bind_local_instructions,
            visible_indexes_local_instructions,
            build_plan_local_instructions,
            projection_labels_local_instructions,
            projection_executor,
            dispatch_result_local_instructions,
            total_local_instructions,
        })
    }

    // Execute one lowered grouped SQL SELECT command through the shared
    // structural grouped runtime and package the page for dispatch consumers.
    #[inline(never)]
    pub(in crate::db::session::sql::dispatch) fn execute_lowered_sql_grouped_dispatch_select_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
        columns: Vec<String>,
    ) -> Result<SqlDispatchResult, QueryError> {
        let structural = Self::structural_query_from_lowered_select(select, authority)?;
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
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
        let row_count = u32::try_from(page.rows.len()).unwrap_or(u32::MAX);

        Ok(SqlDispatchResult::Grouped {
            columns,
            rows: page.rows,
            row_count,
            next_cursor,
        })
    }

    // Execute one lowered SQL DELETE command through the shared structural
    // delete projection path and keep the outward boundary in row-parts form.
    fn execute_lowered_sql_delete_projection_core(
        &self,
        delete: &LoweredBaseQueryShape,
        authority: EntityAuthority,
    ) -> Result<SqlQuerySurfaceRowParts, QueryError> {
        let structural = bind_lowered_sql_delete_query_structural(
            authority.model(),
            delete.clone(),
            MissingRowPolicy::Ignore,
        );
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
        let deleted = execute_sql_delete_projection_for_canister(&self.db, authority, plan)
            .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows).map_err(QueryError::execute)?;

        Ok((
            projection_labels_from_fields(authority.fields()),
            rows,
            row_count,
        ))
    }

    // Execute one lowered SQL DELETE command through the shared structural
    // delete projection path and package it for the general dispatch surface.
    pub(in crate::db::session::sql::dispatch) fn execute_lowered_sql_dispatch_delete_core(
        &self,
        delete: &LoweredBaseQueryShape,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        self.execute_lowered_sql_delete_projection_core(delete, authority)
            .map(|(columns, rows, row_count)| {
                SqlProjectionPayload::new(columns, rows, row_count).into_dispatch_result()
            })
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
