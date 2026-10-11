//! Module: db::session::sql::resumable_update
//! Responsibility: SQL mutation-job parsing, policy admission and accepted intent lowering.
//! Does not own: engine checkpoint encoding, bounded advances or progress transitions.
//! Boundary: admitted SQL scope and fixed patch -> existing executor mutation-job preparation.

use crate::{
    db::{
        DbSession, MutationJobError, MutationJobId, QueryError,
        data::AcceptedFixedUpdatePatch,
        executor::mutation::job::{PreparedMutationJobStart, prepare_accepted_mutation_job_start},
        query::preparation::PreparationWork,
        registry::StoreRuntimeStorageMode,
        schema::AcceptedRowLayoutRuntimeContract,
        session::sql::{
            SqlResumableUpdatePolicyReport, SqlStatementDispatch, SqlUpdatePolicyRejection,
            classify_sql_resumable_update_policy, sql_statement_dispatch,
            with_accepted_sql_update_policy_context,
        },
        sql::parser::SqlWriteValue,
    },
    traits::CanisterKind,
    types::{CurrentTimestamp, Timestamp},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session) fn prepare_mutation_job_start(
        &self,
        job_id: MutationJobId,
        sql: &str,
    ) -> Result<PreparedMutationJobStart, MutationJobError> {
        let operation_timestamp = Timestamp::now();
        let dispatch =
            sql_statement_dispatch(sql).map_err(|_| MutationJobError::IneligibleIntent)?;
        self.prepare_sql_mutation_job_start(job_id, &dispatch, operation_timestamp)
    }

    fn prepare_sql_mutation_job_start(
        &self,
        job_id: MutationJobId,
        dispatch: &SqlStatementDispatch<'_>,
        operation_timestamp: Timestamp,
    ) -> Result<PreparedMutationJobStart, MutationJobError> {
        let lowered = (|| -> Result<_, QueryError> {
            let entity_name = dispatch
                .entity_name()
                .ok_or_else(QueryError::unsupported_query)?;
            let catalog = self
                .accepted_schema_catalog_context_for_entity_name(Some(entity_name))
                .map_err(QueryError::execute)?;
            let identity = catalog.identity();
            let store = self
                .db
                .recovered_store(identity.store_path())
                .map_err(QueryError::execute)?;
            if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
                return Err(QueryError::sql_write_boundary(
                    SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore,
                ));
            }
            let descriptor =
                AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
                    .map_err(QueryError::execute)?;
            let report = with_accepted_sql_update_policy_context(&descriptor, |context| {
                PreparationWork::run(
                    self.db.request_execution_scope(),
                    icydb_diagnostic_code::DiagnosticExecutionLane::Mutation,
                    |work| {
                        classify_sql_resumable_update_policy(
                            dispatch,
                            catalog.snapshot().persisted_snapshot().entity_name(),
                            context,
                            work,
                        )
                    },
                )
            })?;
            let plan = require_resumable_update_plan(report)?;
            // Durable jobs persist one fixed patch. Never admit expressions that
            // could depend on already changed rows after interruption or retry.
            if plan
                .statement()
                .assignments
                .iter()
                .any(|assignment| matches!(assignment.value, SqlWriteValue::Expression(_)))
            {
                return Err(QueryError::sql_write_boundary(
                    SqlWriteBoundaryCode::ResumableUpdateExpressionUnsupported,
                ));
            }
            let selector =
                self.sql_update_selector_query(catalog.accepted_schema_info(), plan.statement())?;
            let patch = Self::sql_update_fixed_patch(&descriptor, plan.statement())?;
            let fixed_patch = AcceptedFixedUpdatePatch::from_update_intent(
                identity.entity_path(),
                identity.entity_tag().value(),
                descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
                catalog.fingerprint(),
                catalog.accepted_row_constraints(),
                &patch,
            )
            .map_err(QueryError::execute)?;
            Ok((catalog, store, selector, fixed_patch))
        })()
        .map_err(MutationJobError::from_query_error)?;
        let (catalog, store, selector, fixed_patch) = lowered;
        prepare_accepted_mutation_job_start(
            job_id,
            &catalog,
            &store,
            &selector,
            &fixed_patch,
            operation_timestamp,
        )
    }
}

fn require_resumable_update_plan(
    report: SqlResumableUpdatePolicyReport,
) -> Result<crate::db::session::sql::SqlTrustedResumableUpdatePlan, QueryError> {
    let rejection = match report {
        Ok(plan) => return Ok(plan),
        Err(rejection) => rejection,
    };

    // Keep route-specific rejection boundaries before projecting shared diagnostics.
    match rejection {
        SqlUpdatePolicyRejection::ExactWindowUnsupported => Err(QueryError::unsupported_query()),
        rejection => Err(rejection.into_query_error()),
    }
}
