use super::{
    SqlWriteMutationExecution, reject_explicit_sql_write_to_generated_field,
    reject_explicit_sql_write_to_managed_field, require_sql_write_policy_plan,
    sql_exact_update_candidate_bounds, sql_update_candidate_bounds,
    sql_write_input_for_accepted_field, sql_write_key_from_component_literals,
    sql_write_key_from_literal, sql_write_patch_set_accepted_field,
    sql_write_patch_set_update_default,
};
use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, QueryError,
        data::AcceptedMutationIntentPatch,
        executor::{MutationMode, StructuralMutationTargetKey, StructuralProjectionScanBudget},
        query::intent::StructuralQuery,
        schema::AcceptedRowLayoutRuntimeContract,
        session::{
            AcceptedSchemaCatalogContext,
            sql::{
                SqlExactUpdatePolicy, SqlExactUpdatePolicyRejection, SqlPublicBoundedUpdatePlan,
                SqlPublicPrimaryKeyUpdatePlan, SqlStatementResult, SqlTrustedExactUpdatePlan,
                SqlUpdateExposurePolicy, SqlUpdatePolicyRejection, SqlUpdatePolicyReport,
                SqlValidatedUpdatePlan, classify_sql_update_policy_for_entity,
                with_accepted_sql_update_policy_context, write_policy::SqlWriteExecutionBounds,
            },
        },
        sql::{
            lowering::bind_sql_update_selector_query_structural_with_schema,
            parser::{SqlUpdateStatement, SqlWriteValue},
        },
    },
    entity::EntityKind,
    metrics::sink::SqlWriteKind,
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
    value::Value,
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

fn require_sql_exact_update_plan(
    report: SqlUpdatePolicyReport,
) -> Result<SqlTrustedExactUpdatePlan, QueryError> {
    if let Some(SqlValidatedUpdatePlan::TrustedExact(plan)) = report.plan {
        return Ok(plan);
    }

    let boundary = match report.rejection {
        Some(SqlUpdatePolicyRejection::MissingWhere) => {
            SqlWriteBoundaryCode::UpdateMissingWherePredicate
        }
        Some(SqlUpdatePolicyRejection::PrimaryKeyMutation) => {
            SqlWriteBoundaryCode::UpdatePrimaryKeyMutation
        }
        Some(SqlUpdatePolicyRejection::GeneratedFieldMutation) => {
            SqlWriteBoundaryCode::ExplicitGeneratedField
        }
        Some(SqlUpdatePolicyRejection::ManagedFieldMutation) => {
            SqlWriteBoundaryCode::ExplicitManagedField
        }
        Some(SqlUpdatePolicyRejection::ExactWindowUnsupported) => {
            SqlWriteBoundaryCode::ExactUpdateWindowUnsupported
        }
        Some(
            SqlUpdatePolicyRejection::NotUpdate
            | SqlUpdatePolicyRejection::PrimaryKeyProofFailed
            | SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder
            | SqlUpdatePolicyRejection::DescendingOrder
            | SqlUpdatePolicyRejection::MissingLimit
            | SqlUpdatePolicyRejection::OffsetUnsupported
            | SqlUpdatePolicyRejection::LimitTooHigh
            | SqlUpdatePolicyRejection::ResumableWindowUnsupported
            | SqlUpdatePolicyRejection::ResumableReturningUnsupported,
        )
        | None => return Err(QueryError::unsupported_query()),
    };

    Err(QueryError::sql_write_boundary(boundary))
}

/// Frozen execution contract selected before candidate collection begins.
///
/// The variant keeps prefix row bounds and exact completion proof attached to
/// the selector so downstream mutation staging cannot reinterpret the call.
#[derive(Clone, Copy)]
enum SqlUpdateExecutionContract {
    /// Maintained public primary-key or intentional-prefix update bounds.
    Validated(SqlWriteExecutionBounds),
    /// Exact complete-set assertion and its independently bounded execution.
    Exact {
        policy: SqlExactUpdatePolicy,
        bounds: SqlWriteExecutionBounds,
    },
}

impl SqlUpdateExecutionContract {
    const fn candidate_bounds(self) -> super::SqlWriteCandidateBounds {
        match self {
            Self::Validated(bounds) => sql_update_candidate_bounds(bounds),
            Self::Exact { policy, .. } => sql_exact_update_candidate_bounds(policy),
        }
    }

    fn selector(self, selector: StructuralQuery) -> StructuralQuery {
        match self {
            Self::Exact { policy, .. } => selector.limit(policy.selection_limit()),
            Self::Validated(_) => selector,
        }
    }

    const fn returning_bounds(
        self,
        returning_requested: bool,
    ) -> Option<crate::db::session::sql::write_policy::SqlWriteReturningBounds> {
        if !returning_requested {
            return None;
        }

        match self {
            Self::Validated(bounds) | Self::Exact { bounds, .. } => Some(bounds.returning),
        }
    }

    fn scan_budget(self) -> Result<Option<StructuralProjectionScanBudget>, QueryError> {
        let Self::Exact { .. } = self else {
            return Ok(None);
        };

        StructuralProjectionScanBudget::try_new(SqlExactUpdatePolicy::scan_budget())
            .map(Some)
            .ok_or_else(QueryError::invariant)
    }
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql) fn sql_structural_patch(
        descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
        statement: &SqlUpdateStatement,
    ) -> Result<AcceptedMutationIntentPatch, QueryError> {
        let mut patch = AcceptedMutationIntentPatch::new();
        for assignment in &statement.assignments {
            if descriptor.is_primary_key_field_name(assignment.field.as_str()) {
                return Err(QueryError::sql_write_boundary(
                    SqlWriteBoundaryCode::UpdatePrimaryKeyMutation,
                ));
            }
            patch = match &assignment.value {
                SqlWriteValue::Literal(value) => {
                    reject_explicit_sql_write_to_generated_field(
                        descriptor,
                        assignment.field.as_str(),
                    )?;
                    reject_explicit_sql_write_to_managed_field(
                        descriptor,
                        assignment.field.as_str(),
                    )?;
                    let input = sql_write_input_for_accepted_field(
                        descriptor,
                        assignment.field.as_str(),
                        value,
                    )?;
                    sql_write_patch_set_accepted_field(
                        descriptor,
                        patch,
                        assignment.field.as_str(),
                        input,
                    )?
                }
                SqlWriteValue::Default => sql_write_patch_set_update_default(
                    descriptor,
                    patch,
                    assignment.field.as_str(),
                )?,
            };
        }

        Ok(patch)
    }

    pub(in crate::db::session::sql) fn sql_update_selector_query<E>(
        schema_info: &crate::db::schema::SchemaInfo,
        statement: &SqlUpdateStatement,
    ) -> Result<StructuralQuery, QueryError>
    where
        E: PersistedRow<Canister = C>,
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

    fn execute_sql_update_statement_with_contract<E>(
        &self,
        statement: &SqlUpdateStatement,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        execution_contract: SqlUpdateExecutionContract,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            catalog,
            statement.returning.as_ref(),
            |catalog, descriptor| {
                let (authority, schema_info) =
                    Self::accepted_sql_write_authority_schema_info::<E>(catalog)?;
                let selector = execution_contract.selector(Self::sql_update_selector_query::<E>(
                    &schema_info,
                    statement,
                )?);
                let patch = Self::sql_structural_patch(&descriptor, statement)?;
                let write_context =
                    SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now());
                let candidate_bounds = execution_contract.candidate_bounds();
                let scan_budget = execution_contract.scan_budget()?;
                let collection = self
                    .collect_bounded_sql_write_candidate_collection_from_structural_query(
                        catalog.snapshot(),
                        authority,
                        &selector,
                        candidate_bounds,
                        scan_budget,
                        |row| {
                            let key =
                                Self::sql_write_key_from_projected_row::<E>(&descriptor, row)?;

                            Ok((StructuralMutationTargetKey::expected(key), patch.clone()))
                        },
                    )?;
                self.execute_sql_write_mutation_batch::<E>(
                    catalog,
                    &descriptor,
                    SqlWriteMutationExecution::from_bounded_collection(
                        collection,
                        candidate_bounds,
                        SqlWriteKind::Update,
                        MutationMode::Update,
                        write_context,
                        execution_contract.returning_bounds(statement.returning.is_some()),
                    )?,
                    statement.returning.as_ref(),
                )
            },
        )
    }

    fn schema_derived_sql_update_report<E>(
        &self,
        sql: &str,
        policy: SqlUpdateExposurePolicy,
    ) -> Result<SqlUpdatePolicyReport, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            None,
            None,
            |catalog, descriptor| {
                with_accepted_sql_update_policy_context(&descriptor, |context| {
                    classify_sql_update_policy_for_entity(
                        sql,
                        catalog.snapshot().persisted_snapshot().entity_name(),
                        policy,
                        context,
                    )
                })
            },
        )
    }

    fn schema_derived_sql_update_plan<E>(
        &self,
        sql: &str,
        policy: SqlUpdateExposurePolicy,
    ) -> Result<SqlValidatedUpdatePlan, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let report = self.schema_derived_sql_update_report::<E>(sql, policy)?;

        require_sql_write_policy_plan(report.plan)
    }

    /// Execute a policy-validated public primary-key SQL `UPDATE` plan.
    ///
    /// This adapter intentionally accepts only the primary-key validated plan
    /// variant, so generated/public write surfaces cannot route broader
    /// session-current or bounded/admin plans through this at-most-one-row
    /// execution path by accident.
    #[doc(hidden)]
    pub(in crate::db) fn execute_validated_sql_public_primary_key_update<E>(
        &self,
        plan: &SqlPublicPrimaryKeyUpdatePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_update_statement_with_contract::<E>(
            plan.statement(),
            None,
            SqlUpdateExecutionContract::Validated(plan.execution_bounds()),
        )
    }

    /// Execute a policy-validated bounded deterministic SQL `UPDATE` plan.
    #[doc(hidden)]
    pub(in crate::db) fn execute_validated_sql_public_bounded_update<E>(
        &self,
        plan: &SqlPublicBoundedUpdatePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_update_statement_with_contract::<E>(
            plan.statement(),
            None,
            SqlUpdateExecutionContract::Validated(plan.execution_bounds()),
        )
    }

    /// Execute a validated exact complete-set SQL `UPDATE` plan.
    #[doc(hidden)]
    pub(in crate::db) fn execute_validated_sql_trusted_exact_update<E>(
        &self,
        plan: &SqlTrustedExactUpdatePlan,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_update_statement_with_contract::<E>(
            plan.statement(),
            None,
            SqlUpdateExecutionContract::Exact {
                policy: plan.policy(),
                bounds: plan.execution_bounds(),
            },
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
        E: PersistedRow<Canister = C>,
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
        E: PersistedRow<Canister = C>,
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

    /// Execute one intentional primary-key-ordered prefix SQL `UPDATE`.
    ///
    /// The statement's positive `LIMIT` selects the prefix to mutate. This
    /// contract does not claim that every matching row was updated.
    pub fn execute_trusted_sql_prefix_update<E>(
        &self,
        sql: &str,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        self.execute_sql_public_bounded_update::<E>(sql)
    }

    /// Execute one trusted exact complete-set SQL `UPDATE`.
    ///
    /// `require_affected_at_most` is a caller assertion, not a selection
    /// window. Selection follows authoritative primary-key traversal and
    /// rejects before mutation when either the affected-row assertion or the
    /// separate scanned-key budget is exceeded. Both current per-call ceilings
    /// are 4,096.
    pub fn execute_trusted_sql_exact_update<E>(
        &self,
        sql: &str,
        require_affected_at_most: u32,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let policy =
            SqlExactUpdatePolicy::try_new(require_affected_at_most).map_err(|rejection| {
                QueryError::sql_write_boundary(match rejection {
                    SqlExactUpdatePolicyRejection::AssertionRequired => {
                        SqlWriteBoundaryCode::ExactUpdateAssertionRequired
                    }
                    SqlExactUpdatePolicyRejection::AssertionTooHigh => {
                        SqlWriteBoundaryCode::ExactUpdateAssertionTooHigh
                    }
                })
            })?;
        let report = self.schema_derived_sql_update_report::<E>(
            sql,
            SqlUpdateExposurePolicy::TrustedExact(policy),
        )?;
        let plan = require_sql_exact_update_plan(report)?;

        self.execute_validated_sql_trusted_exact_update::<E>(&plan)
    }
}
