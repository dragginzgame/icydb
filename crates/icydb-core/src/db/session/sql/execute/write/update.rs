use super::{
    accepted_sql_write_save_contract, checked_accepted_write_descriptor, record_sql_write_metrics,
    reject_explicit_sql_write_to_generated_field, reject_explicit_sql_write_to_managed_field,
    sql_returning_rows, sql_write_key_from_component_literals, sql_write_key_from_literal,
    sql_write_patch_set_accepted_field, sql_write_value_for_accepted_field,
    usize_to_u64_saturating,
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        data::StructuralPatch,
        executor::MutationMode,
        query::intent::StructuralQuery,
        schema::{AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField},
        session::sql::{
            SqlPublicBoundedUpdatePlan, SqlPublicPrimaryKeyUpdatePlan, SqlStatementResult,
            SqlUpdateExposurePolicy, SqlUpdatePolicyContext, SqlUpdateReturningBounds,
            SqlValidatedUpdatePlan, classify_sql_update_policy,
            execute::write_returning::{
                sql_write_statement_result, validate_sql_returning_bounds,
                validate_sql_returning_projection_fields,
            },
        },
        sql::{
            lowering::bind_sql_update_selector_query_structural_with_schema,
            parser::SqlUpdateStatement,
        },
    },
    metrics::sink::SqlWriteKind,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, EntityKind, EntityValue},
    types::Timestamp,
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

impl<C: CanisterKind> DbSession<C> {
    fn sql_structural_patch(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralPatch, QueryError> {
        let mut patch = StructuralPatch::new();
        for assignment in &statement.assignments {
            if descriptor.is_primary_key_field_name(assignment.field.as_str()) {
                return Err(QueryError::sql_write_boundary(
                    SqlWriteBoundaryCode::UpdatePrimaryKeyMutation,
                ));
            }
            reject_explicit_sql_write_to_generated_field(descriptor, assignment.field.as_str())?;
            reject_explicit_sql_write_to_managed_field(descriptor, assignment.field.as_str())?;
            let normalized = sql_write_value_for_accepted_field(
                descriptor,
                assignment.field.as_str(),
                &assignment.value,
            )?;

            patch = sql_write_patch_set_accepted_field(
                descriptor,
                patch,
                assignment.field.as_str(),
                normalized,
            )?;
        }

        Ok(patch)
    }

    fn sql_update_selector_query<E>(
        schema_info: &crate::db::schema::SchemaInfo,
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralQuery, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        if schema_info.primary_key_names().is_empty() {
            return Err(QueryError::invariant());
        }
        let primary_key_names = schema_info
            .primary_key_names()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let selector = bind_sql_update_selector_query_structural_with_schema(
            E::MODEL,
            statement,
            MissingRowPolicy::Ignore,
            schema_info,
        )
        .map_err(QueryError::from_sql_lowering_error)?;

        Ok(selector.select_fields(primary_key_names))
    }

    fn sql_write_key_from_projected_row<E>(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        row: &[Value],
    ) -> Result<E::Key, QueryError>
    where
        E: EntityKind,
    {
        let primary_key_names = descriptor.primary_key_names();
        if primary_key_names.len() == 1 {
            let Some(value) = row.first() else {
                return Err(QueryError::invariant());
            };

            return sql_write_key_from_literal::<E>(descriptor, value);
        }

        if row.len() != primary_key_names.len() {
            return Err(QueryError::invariant());
        }

        sql_write_key_from_component_literals::<E>(descriptor, row)
    }

    pub(in crate::db::session::sql::execute) fn execute_sql_update_statement<E>(
        &self,
        statement: &SqlUpdateStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_sql_update_statement_with_returning_bounds::<E>(statement, None)
    }

    fn execute_sql_update_statement_with_returning_bounds<E>(
        &self,
        statement: &SqlUpdateStatement,
        returning_bounds: Option<SqlUpdateReturningBounds>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;
        validate_sql_returning_projection_fields(&descriptor, statement.returning.as_ref())?;
        let authority = Self::accepted_entity_authority_for_schema::<E>(&schema)
            .map_err(QueryError::execute)?;
        let schema_info = authority
            .accepted_schema_info()
            .ok_or_else(QueryError::invariant)?;
        let save_schema_info = schema_info.clone();
        let selector = Self::sql_update_selector_query::<E>(schema_info, statement)?;
        let patch = Self::sql_structural_patch(&descriptor, statement)?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
        let (payload, _) = self
            .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                selector, authority, &schema,
            )?;
        let (_, _, projected_rows, _) = payload.into_components();
        let matched_rows = usize_to_u64_saturating(projected_rows.len());
        let mut rows = Vec::with_capacity(projected_rows.len());

        for row in projected_rows {
            let key = Self::sql_write_key_from_projected_row::<E>(&descriptor, row.as_slice())?;

            rows.push((key, patch.clone()));
        }
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        ) = accepted_sql_write_save_contract::<E>(&schema, &descriptor, Some(save_schema_info))?;
        let entities = self
            .execute_save_with_checked_accepted_row_contract::<E, _, _>(
                row_decode_contract,
                accepted_schema_info,
                accepted_schema_fingerprint,
                |save| {
                    save.apply_internal_lowered_structural_mutation_batch_with_precommit(
                        MutationMode::Update,
                        rows,
                        write_context,
                        mutation_row_decode_contract,
                        |entities| {
                            validate_sql_returning_bounds(
                                E::MODEL.name(),
                                entities,
                                statement.returning.as_ref(),
                                &descriptor,
                                returning_bounds,
                            )
                        },
                    )
                },
                std::convert::identity,
            )
            .map_err(QueryError::execute)?;
        let mutated_rows = usize_to_u64_saturating(entities.len());
        record_sql_write_metrics(
            E::PATH,
            SqlWriteKind::Update,
            matched_rows,
            mutated_rows,
            sql_returning_rows(statement.returning.as_ref(), mutated_rows),
        );

        sql_write_statement_result::<E>(entities, statement.returning.as_ref(), &descriptor)
    }

    fn schema_derived_sql_update_plan<E>(
        &self,
        sql: &str,
        policy: SqlUpdateExposurePolicy,
    ) -> Result<SqlValidatedUpdatePlan, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor::<E>(&schema)?;
        let generated_fields = descriptor
            .fields()
            .iter()
            .filter(|field| field.write_policy().insert_generation().is_some())
            .map(AcceptedRowLayoutRuntimeField::name)
            .collect::<Vec<_>>();
        let managed_fields = descriptor
            .fields()
            .iter()
            .filter(|field| field.write_policy().write_management().is_some())
            .map(AcceptedRowLayoutRuntimeField::name)
            .collect::<Vec<_>>();
        let context = SqlUpdatePolicyContext {
            primary_key_fields: descriptor.primary_key_names(),
            generated_fields: generated_fields.as_slice(),
            managed_fields: managed_fields.as_slice(),
            max_public_bounded_limit: 100,
            max_returning_rows: None,
            max_returning_response_bytes: None,
        };
        let report = classify_sql_update_policy(sql, policy, context)?;
        let Some(plan) = report.plan else {
            return Err(QueryError::unsupported_query());
        };

        Ok(plan)
    }

    /// Execute a policy-validated public primary-key SQL `UPDATE` plan.
    ///
    /// This adapter intentionally accepts only the primary-key validated plan
    /// variant, so generated/public write surfaces cannot route broader
    /// session-current or bounded/admin plans through this at-most-one-row
    /// execution path by accident.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_primary_key_update<E>(
        &self,
        plan: &SqlPublicPrimaryKeyUpdatePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_sql_update_statement_with_returning_bounds::<E>(
            plan.statement(),
            Some(plan.returning_bounds),
        )
    }

    /// Execute a policy-validated bounded deterministic SQL `UPDATE` plan.
    #[doc(hidden)]
    pub fn execute_validated_sql_public_bounded_update<E>(
        &self,
        plan: &SqlPublicBoundedUpdatePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.execute_sql_update_statement_with_returning_bounds::<E>(
            plan.statement(),
            Some(plan.returning_bounds),
        )
    }

    /// Classify and execute one public primary-key-only SQL `UPDATE`.
    ///
    /// The policy context is derived from the accepted runtime descriptor, so
    /// callers cannot accidentally validate public SQL against generated model
    /// facts or hand-authored field lists.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_update<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = self.schema_derived_sql_update_plan::<E>(
            sql,
            SqlUpdateExposurePolicy::PublicPrimaryKeyOnly,
        )?;
        let SqlValidatedUpdatePlan::PublicPrimaryKeyOnly(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_primary_key_update::<E>(&plan)
    }

    /// Classify and execute one bounded deterministic public SQL `UPDATE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_update<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = self.schema_derived_sql_update_plan::<E>(
            sql,
            SqlUpdateExposurePolicy::PublicBoundedDeterministic,
        )?;
        let SqlValidatedUpdatePlan::PublicBoundedDeterministic(plan) = plan else {
            return Err(QueryError::invariant());
        };

        self.execute_validated_sql_public_bounded_update::<E>(&plan)
    }
}
