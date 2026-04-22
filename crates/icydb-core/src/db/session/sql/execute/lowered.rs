//! Module: db::session::sql::execute::lowered
//! Responsibility: bind lowered SQL commands onto structural query/aggregate
//! execution and preserve attribution or outward row-shape boundaries.
//! Does not own: lowered SQL parsing or public session API classification.
//! Boundary: keeps lowered-command execution bridges explicit and authority-aware.

use crate::{
    db::{
        DbSession, QueryError,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        session::sql::{SqlStatementResult, projection::grouped_sql_statement_result_from_page},
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
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
}
