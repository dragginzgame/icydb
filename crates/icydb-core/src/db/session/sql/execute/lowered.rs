//! Module: db::session::sql::execute::lowered
//! Responsibility: bind lowered SQL commands onto structural query/aggregate
//! execution and preserve attribution or outward row-shape boundaries.
//! Does not own: lowered SQL parsing or public session API classification.
//! Boundary: keeps lowered-command execution bridges explicit and authority-aware.

#[cfg(feature = "perf-attribution")]
use crate::db::session::sql::{
    SqlProjectionTextExecutorAttribution,
    projection::{
        attribute_sql_projection_text_rows_for_canister, projection_labels_from_projection_spec,
    },
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::{EntityAuthority, pipeline::execute_initial_grouped_rows_for_canister},
        query::intent::StructuralQuery,
        session::sql::{
            SqlStatementResult,
            projection::{
                SqlProjectionPayload, grouped_sql_statement_result,
                projection_labels_from_projection_spec,
            },
        },
        sql::lowering::{LoweredSelectShape, bind_lowered_sql_select_query_structural},
    },
    traits::CanisterKind,
};

///
/// LoweredSqlStatementExecutorAttribution
///
/// LoweredSqlStatementExecutorAttribution breaks the lowered SQL statement
/// executor path into structural bind, visible-index lookup, plan build,
/// projection-label derivation, executor internals, and final statement result
/// packaging.
/// This keeps perf attribution attached to the stable lowered SQL boundary
/// instead of scattering measurement logic across unrelated callers.
///

#[cfg(feature = "perf-attribution")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoweredSqlStatementExecutorAttribution {
    pub bind_local_instructions: u64,
    pub visible_indexes_local_instructions: u64,
    pub build_plan_local_instructions: u64,
    pub projection_labels_local_instructions: u64,
    pub projection_executor: SqlProjectionTextExecutorAttribution,
    pub statement_result_local_instructions: u64,
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
fn measure_statement_result<T, E>(run: impl FnOnce() -> Result<T, E>) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

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
        self.execute_lowered_sql_select_with(
            select,
            authority,
            Self::execute_structural_sql_projection,
        )
    }

    #[cfg(feature = "perf-attribution")]
    #[doc(hidden)]
    pub fn attribute_lowered_sql_statement_query_for_authority(
        &self,
        lowered: &LoweredSqlCommand,
        authority: EntityAuthority,
    ) -> Result<LoweredSqlStatementExecutorAttribution, QueryError> {
        let Some(LoweredSqlQuery::Select(select)) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "executor attribution currently supports lowered SQL SELECT only",
            ));
        };

        let (bind_local_instructions, structural) = measure_statement_result(|| {
            Self::structural_query_from_lowered_select(select, authority)
        });
        let structural = structural?;

        let (visible_indexes_local_instructions, visible_indexes) =
            measure_statement_result(|| {
                self.visible_indexes_for_store_model(authority.store_path(), authority.model())
            });
        let visible_indexes = visible_indexes?;

        let (build_plan_local_instructions, plan) = measure_statement_result(|| {
            structural.build_plan_with_visible_indexes(&visible_indexes)
        });
        let plan = plan?;

        let (projection_labels_local_instructions, columns) = measure_statement_result(|| {
            let projection = plan.projection_spec(authority.model());

            Ok::<Vec<String>, QueryError>(projection_labels_from_projection_spec(&projection))
        });
        let columns = columns?;

        let projection_executor =
            attribute_sql_projection_text_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;

        let (statement_result_local_instructions, statement_result) =
            measure_statement_result(|| {
                Ok::<SqlStatementResult, QueryError>(SqlStatementResult::ProjectionText {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                })
            });
        let _statement_result = statement_result?;

        let total_local_instructions = bind_local_instructions
            .saturating_add(visible_indexes_local_instructions)
            .saturating_add(build_plan_local_instructions)
            .saturating_add(projection_labels_local_instructions)
            .saturating_add(projection_executor.total)
            .saturating_add(statement_result_local_instructions);

        Ok(LoweredSqlStatementExecutorAttribution {
            bind_local_instructions,
            visible_indexes_local_instructions,
            build_plan_local_instructions,
            projection_labels_local_instructions,
            projection_executor,
            statement_result_local_instructions,
            total_local_instructions,
        })
    }

    // Execute one lowered grouped SQL SELECT command through the shared
    // structural grouped runtime and package the page for statement consumers.
    #[inline(never)]
    pub(in crate::db::session::sql::execute) fn execute_lowered_sql_grouped_statement_select_core(
        &self,
        select: LoweredSelectShape,
        authority: EntityAuthority,
    ) -> Result<SqlStatementResult, QueryError> {
        let structural = Self::structural_query_from_lowered_select(select, authority)?;
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
        let columns =
            projection_labels_from_projection_spec(&plan.projection_spec(authority.model()));
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
            page.rows,
            next_cursor,
        ))
    }
}
