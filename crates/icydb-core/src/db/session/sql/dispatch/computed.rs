use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        executor::EntityAuthority,
        query::intent::StructuralQuery,
        session::sql::SqlDispatchResult,
        session::sql::computed_projection,
        sql::lowering::{
            LoweredSqlQuery, apply_lowered_select_shape, lower_sql_command_from_prepared_statement,
            prepare_sql_statement,
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
        // Phase 1: lower the rewritten field-only base query through the
        // shared SQL preparation/lowering path.
        let lowered = lower_sql_command_from_prepared_statement(
            prepare_sql_statement(plan.cloned_base_statement(), E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.query().cloned() else {
            return Err(QueryError::unsupported_query(
                "computed SQL projection requires a lowered SELECT statement",
            ));
        };

        // Phase 2: execute the base field-only projection and then apply the
        // requested transforms without reopening generic expression ownership.
        let structural = apply_lowered_select_shape(
            StructuralQuery::new(E::MODEL, MissingRowPolicy::Ignore),
            select,
        )
        .map_err(QueryError::from_sql_lowering_error)?;
        let base_payload =
            self.execute_structural_sql_projection(structural, EntityAuthority::for_type::<E>())?;
        let projected =
            computed_projection::apply_computed_sql_projection_payload(base_payload, &plan)?;

        Ok(projected.into_dispatch_result())
    }

    // Render one supported computed SQL projection through the existing shared
    // EXPLAIN machinery by rewriting the narrowed session-owned lane back onto
    // its base field-only SELECT authority.
    pub(in crate::db::session::sql::dispatch) fn explain_computed_sql_projection_dispatch<E>(
        mode: SqlExplainMode,
        plan: computed_projection::SqlComputedProjectionPlan,
    ) -> Result<String, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
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
            prepare_sql_statement(explain_statement, E::MODEL.name())
                .map_err(QueryError::from_sql_lowering_error)?,
            E::MODEL.primary_key.name,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        lowered.explain_for_model(E::MODEL)
    }
}
