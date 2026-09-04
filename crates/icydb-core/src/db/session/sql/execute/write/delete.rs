use super::{require_sql_write_policy_plan, sql_write_candidate_bounds};
use crate::{
    db::{
        DbSession, MissingRowPolicy, QueryError,
        data::AcceptedMutationIntentPatch,
        query::intent::StructuralQuery,
        schema::SchemaInfo,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlDeleteExposurePolicy, SqlDeletePolicyContext, SqlPublicBoundedDeletePlan,
                SqlPublicPrimaryKeyDeletePlan, SqlStatementDispatch, SqlStatementResult,
                SqlValidatedDeletePlan, classify_sql_delete_statement_policy,
                execute::write_returning::{
                    projection_labels_from_accepted_write_descriptor,
                    sql_returning_statement_projection, validate_sql_materialized_returning_bounds,
                },
                write_policy::SqlWriteExecutionBounds,
            },
            structural_data_key_from_runtime_values,
        },
        sql::{
            lowering::bind_sql_delete_statement_structural_with_schema,
            parser::{SqlDeleteStatement, SqlReturningProjection},
        },
    },
    traits::CanisterKind,
};

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql::execute) fn execute_sql_delete_statement(
        &self,
        query: &StructuralQuery,
        returning: Option<&SqlReturningProjection>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
    ) -> Result<SqlStatementResult, QueryError> {
        self.execute_sql_delete_statement_with_execution_bounds(query, returning, catalog, None)
    }

    fn execute_sql_delete_statement_with_execution_bounds(
        &self,
        query: &StructuralQuery,
        returning: Option<&SqlReturningProjection>,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        execution_bounds: Option<SqlWriteExecutionBounds>,
    ) -> Result<SqlStatementResult, QueryError> {
        let entity_name = catalog
            .map(|catalog| {
                catalog
                    .snapshot()
                    .persisted_snapshot()
                    .entity_name()
                    .to_string()
            })
            .ok_or_else(QueryError::invariant)?;
        self.with_checked_accepted_write_descriptor_for_returning(
            catalog,
            Some(entity_name.as_str()),
            returning,
            |catalog, descriptor| {
                let (authority, _) = Self::accepted_sql_write_authority_schema_info(catalog);
                let primary_names = descriptor.primary_key_names();
                let selector = query
                    .clone()
                    .into_load_selection()
                    .select_fields(primary_names.iter().copied());
                let bounds = sql_write_candidate_bounds(execution_bounds);
                let entity_tag = catalog.identity().entity_tag();
                let collection = self
                    .collect_bounded_sql_write_candidate_collection_from_structural_query(
                        catalog.snapshot(),
                        authority,
                        &selector,
                        bounds,
                        None,
                        |row| {
                            let key =
                                structural_data_key_from_runtime_values(entity_tag, row.to_vec())
                                    .map_err(QueryError::execute)?;
                            Ok((key, AcceptedMutationIntentPatch::new()))
                        },
                    )?;
                let keys = collection
                    .into_batch()
                    .into_rows()
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect();
                let columns = projection_labels_from_accepted_write_descriptor(&descriptor);
                let rows = self
                    .execute_accepted_structural_delete_batch(catalog, &descriptor, keys, |rows| {
                        let Some(returning) = returning else {
                            return Ok(());
                        };
                        validate_sql_materialized_returning_bounds(
                            entity_name.as_str(),
                            columns.as_slice(),
                            rows,
                            u32::try_from(rows.len()).unwrap_or(u32::MAX),
                            returning,
                            catalog.enum_catalog(),
                            execution_bounds.map(|bounds| bounds.returning),
                        )
                    })
                    .map_err(QueryError::execute)?;
                let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
                match returning {
                    None => Ok(SqlStatementResult::Count { row_count }),
                    Some(returning) => sql_returning_statement_projection(
                        catalog.enum_catalog(),
                        columns,
                        rows,
                        row_count,
                        returning,
                    ),
                }
            },
        )
    }

    fn sql_delete_query_from_statement(
        schema_info: &SchemaInfo,
        statement: &SqlDeleteStatement,
    ) -> Result<StructuralQuery, QueryError> {
        bind_sql_delete_statement_structural_with_schema(
            statement.clone(),
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)
    }

    fn schema_derived_sql_delete_plan(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
        policy: SqlDeleteExposurePolicy,
    ) -> Result<SqlValidatedDeletePlan, QueryError> {
        let entity_name = dispatch.entity_name();
        self.with_checked_accepted_write_descriptor_for_returning(
            None,
            entity_name,
            None,
            |_catalog, descriptor| {
                let context =
                    SqlDeletePolicyContext::public_generated(descriptor.primary_key_names());
                let report =
                    classify_sql_delete_statement_policy(dispatch.statement(), policy, context);
                require_sql_write_policy_plan(report.plan)
            },
        )
    }

    fn execute_validated_sql_delete_statement(
        &self,
        statement: &SqlDeleteStatement,
        execution_bounds: SqlWriteExecutionBounds,
    ) -> Result<SqlStatementResult, QueryError> {
        self.with_checked_accepted_write_descriptor_for_returning(
            None,
            Some(statement.entity.as_str()),
            statement.returning.as_ref(),
            |catalog, _descriptor| {
                let (_authority, schema_info) =
                    Self::accepted_sql_write_authority_schema_info(catalog);
                let query = Self::sql_delete_query_from_statement(&schema_info, statement)?;

                self.execute_sql_delete_statement_with_execution_bounds(
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
    pub(in crate::db) fn execute_validated_sql_public_primary_key_delete(
        &self,
        plan: &SqlPublicPrimaryKeyDeletePlan,
    ) -> Result<SqlStatementResult, QueryError> {
        self.execute_validated_sql_delete_statement(plan.statement(), plan.execution_bounds())
    }

    /// Execute a policy-validated bounded deterministic SQL `DELETE` plan.
    #[doc(hidden)]
    pub(in crate::db) fn execute_validated_sql_public_bounded_delete(
        &self,
        plan: &SqlPublicBoundedDeletePlan,
    ) -> Result<SqlStatementResult, QueryError> {
        self.execute_validated_sql_delete_statement(plan.statement(), plan.execution_bounds())
    }

    /// Classify and execute one public primary-key-only delete from a parsed dispatch.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_delete(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<SqlStatementResult, QueryError> {
        let plan = self.schema_derived_sql_delete_plan(
            dispatch,
            SqlDeleteExposurePolicy::PublicPrimaryKeyOnly,
        )?;
        let SqlValidatedDeletePlan::PublicPrimaryKeyOnly(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_primary_key_delete(&plan)
    }

    /// Classify and execute one bounded deterministic public delete from a parsed dispatch.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_delete(
        &self,
        dispatch: &SqlStatementDispatch<'_>,
    ) -> Result<SqlStatementResult, QueryError> {
        let plan = self.schema_derived_sql_delete_plan(
            dispatch,
            SqlDeleteExposurePolicy::PublicBoundedDeterministic,
        )?;
        let SqlValidatedDeletePlan::PublicBoundedDeterministic(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_bounded_delete(&plan)
    }
}
