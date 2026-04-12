//! Module: db::session::sql::dispatch::computed
//! Responsibility: session-owned dispatch helpers for the bounded computed SQL
//! projection surface that stays outside generic structural planning.
//! Does not own: generic projection planning or executor projection semantics.
//! Boundary: keeps computed select-list dispatch isolated from the structural SQL lane.

use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        executor::EntityAuthority,
        session::sql::{SqlDispatchResult, computed_projection},
        sql::{
            lowering::{
                LoweredSqlQuery, bind_lowered_sql_select_query_structural,
                lower_sql_command_from_prepared_statement, prepare_sql_statement,
            },
            parser::{SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlStatement},
        },
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    // Execute one supported computed SQL projection for one already-resolved
    // dynamic authority so both typed and generated SQL dispatch stay on the
    // same computed-projection execution path.
    pub(in crate::db::session::sql::dispatch) fn execute_computed_sql_projection_dispatch_for_authority(
        &self,
        plan: computed_projection::SqlComputedProjectionPlan,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        // Phase 1: lower the rewritten field-only base query through the
        // shared SQL preparation/lowering path for the resolved dynamic model.
        let lowered = lower_sql_command_from_prepared_statement(
            prepare_sql_statement(plan.cloned_base_statement(), authority.model().name())
                .map_err(QueryError::from_sql_lowering_error)?,
            authority.model().primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "computed SQL projection requires a lowered SELECT statement",
            ));
        };

        // Phase 2: execute the base query through the existing scalar or
        // grouped lane, then apply only the outward computed transform.
        if plan.is_grouped() {
            let grouped = self.execute_lowered_sql_grouped_dispatch_select_core(
                select,
                authority,
                plan.output_labels(),
            )?;
            let SqlDispatchResult::Grouped {
                columns,
                rows,
                row_count,
                next_cursor,
            } = grouped
            else {
                return Err(QueryError::invariant(
                    "grouped computed SQL projection did not produce grouped dispatch payload",
                ));
            };
            let rows =
                computed_projection::apply_computed_sql_projection_grouped_rows(rows, &plan)?;

            return Ok(SqlDispatchResult::Grouped {
                columns,
                rows,
                row_count,
                next_cursor,
            });
        }

        let structural = bind_lowered_sql_select_query_structural(
            authority.model(),
            select,
            MissingRowPolicy::Ignore,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let base_payload = self.execute_structural_sql_projection(structural, authority)?;
        let projected =
            computed_projection::apply_computed_sql_projection_payload(base_payload, &plan)?;

        Ok(projected.into_dispatch_result())
    }

    // Render one supported computed SQL projection explain for one already-
    // resolved dynamic authority so both typed and generated EXPLAIN dispatch
    // stay on the same computed-projection explain path.
    pub(in crate::db::session::sql::dispatch) fn explain_computed_sql_projection_dispatch_for_authority(
        &self,
        mode: SqlExplainMode,
        plan: computed_projection::SqlComputedProjectionPlan,
        authority: EntityAuthority,
    ) -> Result<String, QueryError> {
        let SqlStatement::Select(base_select) = plan.into_base_statement() else {
            return Err(QueryError::invariant(
                "computed SQL projection explain requires a base SELECT statement",
            ));
        };
        let explain_statement = SqlStatement::Explain(SqlExplainStatement {
            mode,
            statement: SqlExplainTarget::Select(base_select),
        });
        let lowered = lower_sql_command_from_prepared_statement(
            prepare_sql_statement(explain_statement, authority.model().name())
                .map_err(QueryError::from_sql_lowering_error)?,
            authority.model().primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        self.explain_lowered_sql_for_authority(&lowered, authority)
    }
}
