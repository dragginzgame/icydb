use super::{
    SqlWriteCandidateAccounting, SqlWriteCandidateBounds, SqlWriteCandidateRows,
    record_sql_write_metrics, require_sql_write_policy_plan,
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        executor::DeleteProjectionBounds,
        query::intent::StructuralQuery,
        schema::SchemaInfo,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
                SqlPublicPrimaryKeyDeletePlan, SqlStatementResult, SqlValidatedDeletePlan,
                classify_sql_delete_policy, combined_optional_row_bound,
                execute::write_returning::{
                    projection_labels_from_accepted_write_descriptor,
                    sql_returning_statement_projection, validate_sql_materialized_returning_bounds,
                },
                write_policy::SqlWriteExecutionBounds,
            },
        },
        sql::{
            lowering::bind_sql_delete_statement_structural_with_schema,
            parser::{SqlDeleteStatement, SqlReturningProjection},
        },
    },
    metrics::sink::SqlWriteKind,
    traits::CanisterKind,
};

fn record_sql_write_delete_metrics(entity_path: &'static str, row_count: u32, returning: bool) {
    record_sql_write_metrics(
        entity_path,
        SqlWriteKind::Delete,
        SqlWriteCandidateAccounting::delete_count(
            SqlWriteCandidateRows::from_delete_count(row_count),
            returning,
        ),
    );
}

const fn sql_delete_candidate_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
    returning: bool,
) -> SqlWriteCandidateBounds {
    let Some(execution_bounds) = execution_bounds else {
        return SqlWriteCandidateBounds::from_max_rows(None);
    };

    if !returning {
        return SqlWriteCandidateBounds::from_max_rows(execution_bounds.max_staged_rows);
    }

    SqlWriteCandidateBounds::from_max_rows(combined_optional_row_bound(
        execution_bounds.max_staged_rows,
        execution_bounds.returning.max_rows,
    ))
}

const fn sql_delete_projection_bounds(
    execution_bounds: Option<SqlWriteExecutionBounds>,
    returning: bool,
) -> DeleteProjectionBounds {
    match sql_delete_candidate_bounds(execution_bounds, returning).max_rows() {
        Some(max_rows) => DeleteProjectionBounds::max_rows(max_rows),
        None => DeleteProjectionBounds::unbounded(),
    }
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql::execute) fn execute_sql_delete_statement<E>(
        &self,
        query: &StructuralQuery,
        returning: Option<&SqlReturningProjection>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_delete_statement_with_execution_bounds::<E>(
            query, returning, catalog, None,
        )
    }

    fn execute_sql_delete_statement_with_execution_bounds<E>(
        &self,
        query: &StructuralQuery,
        returning: Option<&SqlReturningProjection>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        execution_bounds: Option<SqlWriteExecutionBounds>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let typed_query = Query::<E>::from_inner(query.clone());

        // Phase 1: keep pure count deletes on the direct terminal so the
        // delete lane does not hop through projection shaping it will discard.
        match returning {
            None => {
                let (plan, _) = self.cached_prepared_query_plan_for_entity::<E>(&typed_query)?;
                self.ensure_prepared_query_plan_is_current(&plan)?;
                let bounds = sql_delete_projection_bounds(execution_bounds, false);
                let row_count = self
                    .with_metrics(|| {
                        self.delete_executor::<E>()
                            .execute_count_with_bounds(plan, bounds)
                    })
                    .map_err(QueryError::execute)?;
                record_sql_write_delete_metrics(E::PATH, row_count, false);

                Ok(SqlStatementResult::Count { row_count })
            }
            Some(returning) => {
                self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
                    catalog,
                    Some(returning),
                    |catalog, descriptor| {
                        let columns = projection_labels_from_accepted_write_descriptor(&descriptor);

                        // Phase 2: returning deletes reuse the structural projection
                        // terminal once, then shape the requested outbound row contract
                        // from executor-materialized rows at the SQL write boundary.
                        let (plan, _) =
                            self.cached_prepared_query_plan_for_entity::<E>(&typed_query)?;
                        self.ensure_prepared_query_plan_is_current(&plan)?;
                        let bounds = sql_delete_projection_bounds(execution_bounds, true);
                        let deleted = self
                            .with_metrics(|| {
                                self.delete_executor::<E>()
                                    .execute_structural_projection_with_bounds(
                                        plan,
                                        bounds,
                                        |projection| {
                                            validate_sql_materialized_returning_bounds(
                                                E::MODEL.name(),
                                                columns.as_slice(),
                                                projection.value_rows(),
                                                projection.row_count(),
                                                returning,
                                                catalog.enum_catalog(),
                                                execution_bounds.map(|bounds| bounds.returning),
                                            )
                                        },
                                    )
                            })
                            .map_err(QueryError::execute)?;
                        let (rows, row_count) = deleted.into_rows_and_count();
                        let rows = rows.into_value_rows();
                        record_sql_write_delete_metrics(E::PATH, row_count, true);

                        sql_returning_statement_projection(
                            catalog.enum_catalog(),
                            columns,
                            rows,
                            row_count,
                            returning,
                        )
                    },
                )
            }
        }
    }

    fn sql_delete_query_from_statement<E>(
        schema_info: &SchemaInfo,
        statement: &SqlDeleteStatement,
    ) -> Result<StructuralQuery, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        bind_sql_delete_statement_structural_with_schema(
            E::MODEL,
            statement.clone(),
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)
    }

    fn schema_derived_sql_delete_plan<E>(
        &self,
        sql: &str,
        policy: SqlDeleteExposurePolicy,
    ) -> Result<SqlValidatedDeletePlan, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            None,
            None,
            |_catalog, descriptor| {
                let context =
                    SqlDeletePolicyContext::public_generated(descriptor.primary_key_names());
                let report = classify_sql_delete_policy(sql, policy, context)?;
                require_sql_write_policy_plan(report.plan)
            },
        )
    }

    fn execute_validated_sql_delete_statement<E>(
        &self,
        statement: &SqlDeleteStatement,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            None,
            statement.returning.as_ref(),
            |catalog, _descriptor| {
                let (_authority, schema_info) =
                    Self::accepted_sql_write_authority_schema_info::<E>(catalog)?;
                let query = Self::sql_delete_query_from_statement::<E>(&schema_info, statement)?;

                self.execute_sql_delete_statement_with_execution_bounds::<E>(
                    &query,
                    statement.returning.as_ref(),
                    Some(catalog),
                    Some(execution_bounds),
                )
            },
        )
    }

    /// Execute a policy-validated public primary-key SQL `DELETE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_primary_key_delete<E>(
        &self,
        plan: &SqlPublicPrimaryKeyDeletePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_validated_sql_delete_statement::<E>(plan.statement(), plan.execution_bounds())
    }

    /// Execute a policy-validated bounded deterministic SQL `DELETE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_bounded_delete<E>(
        &self,
        plan: &SqlPublicBoundedDeletePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_validated_sql_delete_statement::<E>(plan.statement(), plan.execution_bounds())
    }

    /// Classify and execute one public primary-key-only SQL `DELETE`.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_delete<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let plan = self.schema_derived_sql_delete_plan::<E>(
            sql,
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        )?;
        let SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_primary_key_delete::<E>(&plan)
    }

    /// Classify and execute one bounded deterministic public SQL `DELETE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_delete<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let plan = self.schema_derived_sql_delete_plan::<E>(
            sql,
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        )?;
        let SqlValidatedDeletePlan::PublicBoundedDeterministic(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_bounded_delete::<E>(&plan)
    }
}

#[cfg(test)]
mod tests {
    use super::sql_delete_candidate_bounds;
    use crate::db::session::sql::{SqlWriteExecutionBounds, SqlWriteReturningBounds};

    #[test]
    fn sql_delete_candidate_bounds_use_tighter_staged_or_returning_cap() {
        let bounds = SqlWriteExecutionBounds {
            max_staged_rows: Some(5),
            returning: SqlWriteReturningBounds {
                max_rows: Some(3),
                max_response_bytes: None,
            },
        };

        assert_eq!(
            sql_delete_candidate_bounds(Some(bounds), false).max_rows(),
            Some(5)
        );
        assert_eq!(
            sql_delete_candidate_bounds(Some(bounds), true).max_rows(),
            Some(3)
        );

        let bounds = SqlWriteExecutionBounds {
            max_staged_rows: Some(2),
            returning: SqlWriteReturningBounds {
                max_rows: Some(4),
                max_response_bytes: None,
            },
        };

        assert_eq!(
            sql_delete_candidate_bounds(Some(bounds), true).max_rows(),
            Some(2)
        );
    }
}
