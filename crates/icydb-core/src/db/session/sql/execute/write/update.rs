use super::{
    SqlWriteMutationExecution, checked_accepted_write_descriptor,
    checked_accepted_write_descriptor_for_returning, reject_explicit_sql_write_to_generated_field,
    reject_explicit_sql_write_to_managed_field, require_sql_write_policy_plan,
    sql_update_candidate_bounds, sql_write_key_from_component_literals, sql_write_key_from_literal,
    sql_write_patch_set_accepted_field, sql_write_value_for_accepted_field,
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
            SqlUpdateExposurePolicy, SqlUpdatePolicyContext, SqlValidatedUpdatePlan,
            classify_sql_update_policy, write_policy::SqlWriteExecutionBounds,
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
        self.execute_sql_update_statement_with_execution_bounds::<E>(statement, None)
    }

    fn execute_sql_update_statement_with_execution_bounds<E>(
        &self,
        statement: &SqlUpdateStatement,
        execution_bounds: Option<SqlWriteExecutionBounds>,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let schema = self
            .ensure_accepted_schema_snapshot::<E>()
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor_for_returning::<E>(
            &schema,
            statement.returning.as_ref(),
        )?;
        let (authority, schema_info) =
            Self::accepted_sql_write_authority_schema_info::<E>(&schema)?;
        let selector = Self::sql_update_selector_query::<E>(&schema_info, statement)?;
        let save_schema_info = schema_info;
        let patch = Self::sql_structural_patch(&descriptor, statement)?;
        let write_context = SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
        let rows = self.collect_sql_write_mutation_batch_from_structural_query(
            &schema,
            authority,
            &selector,
            |row| {
                let key = Self::sql_write_key_from_projected_row::<E>(&descriptor, row)?;

                Ok((key, patch.clone()))
            },
        )?;
        let candidate_rows =
            rows.validate_staged_rows(sql_update_candidate_bounds(execution_bounds))?;
        self.execute_sql_write_mutation_batch::<E>(
            &schema,
            &descriptor,
            SqlWriteMutationExecution {
                rows,
                staged_rows: candidate_rows,
                kind: SqlWriteKind::Update,
                mode: MutationMode::Update,
                context: write_context,
                returning_bounds: execution_bounds.map(|bounds| bounds.returning),
                save_schema_info: Some(save_schema_info),
            },
            statement.returning.as_ref(),
        )
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
        let context = SqlUpdatePolicyContext::public_generated(
            descriptor.primary_key_names(),
            generated_fields.as_slice(),
            managed_fields.as_slice(),
        );
        let report = classify_sql_update_policy(sql, policy, context)?;
        require_sql_write_policy_plan(report.plan)
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
        self.execute_sql_update_statement_with_execution_bounds::<E>(
            plan.statement(),
            Some(plan.execution_bounds()),
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
        self.execute_sql_update_statement_with_execution_bounds::<E>(
            plan.statement(),
            Some(plan.execution_bounds()),
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
