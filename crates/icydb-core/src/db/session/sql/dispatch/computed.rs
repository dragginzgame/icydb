//! Module: db::session::sql::dispatch::computed
//! Responsibility: session-owned dispatch helpers for the bounded computed SQL
//! projection surface that stays outside generic structural planning.
//! Does not own: generic projection planning or executor projection semantics.
//! Boundary: keeps computed select-list dispatch isolated from the structural SQL lane.

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        executor::EntityAuthority,
        session::sql::SqlDispatchResult,
        session::sql::computed_projection,
        sql::lowering::{
            LoweredSqlQuery, bind_lowered_sql_select_query_structural,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::{SqlExplainMode, SqlExplainStatement, SqlExplainTarget, SqlStatement},
    },
    traits::{CanisterKind, EntityValue},
};

impl<C: CanisterKind> DbSession<C> {
    // Execute one supported computed SQL projection through the existing
    // structural field-loading lane and apply the narrow transform bundle at
    // the session boundary.
    pub(in crate::db::session::sql::dispatch) fn execute_computed_sql_projection_dispatch<E>(
        &self,
        plan: computed_projection::SqlComputedProjectionPlan,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_computed_sql_projection_dispatch_for_authority(
            plan,
            EntityAuthority::for_type::<E>(),
        )
    }

    // Execute one supported computed SQL projection for one already-resolved
    // dynamic authority. The generated canister SQL surface uses this lane so
    // it can keep authority lookup dynamic without falling behind typed
    // dispatch on computed text projection support.
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

        // Phase 2: execute the base field-only projection and then apply the
        // requested transforms without reopening generic expression ownership.
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

    // Render one supported computed SQL projection through the existing shared
    // EXPLAIN machinery by rewriting the narrowed session-owned lane back onto
    // its base field-only SELECT authority.
    pub(in crate::db::session::sql::dispatch) fn explain_computed_sql_projection_dispatch<E>(
        &self,
        mode: SqlExplainMode,
        plan: computed_projection::SqlComputedProjectionPlan,
    ) -> Result<String, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.explain_computed_sql_projection_dispatch_for_authority(
            mode,
            plan,
            EntityAuthority::for_type::<E>(),
        )
    }

    // Render one supported computed SQL projection explain for one already-
    // resolved dynamic authority on the generated canister SQL surface.
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
