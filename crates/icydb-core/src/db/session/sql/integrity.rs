//! Module: db::session::sql::integrity
//! Responsibility: lower administrative integrity SQL into the typed request owner.
//! Does not own: integrity semantics, durable jobs, SQL shell routing, or authorization policy.
//! Boundary: parsed integrity SQL -> `IntegrityCheckRequest` -> durable integrity controller.

use crate::{
    db::{
        DbSession, QueryError,
        integrity::{
            IntegrityCheckRequest, IntegrityCheckResult, IntegrityDeepError,
            IntegrityEntityIdentity, IntegrityJobError, IntegrityJobId, IntegrityJobOwner,
            IntegritySubmissionKey,
        },
        sql::{SqlIntegrityStatement, identifier::identifiers_tail_match, parse_integrity_sql},
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::SqlLoweringCode;

/// Typed failure from administrative `CHECK INTEGRITY` SQL.
///
/// Parsing/lowering failures remain distinct from the canonical integrity
/// protocol and engine failures produced after a typed request exists.

#[derive(Debug)]
pub enum SqlIntegrityError {
    /// The canonical integrity controller rejected or could not execute the request.
    Integrity(IntegrityDeepError),

    /// SQL parsing or entity-name lowering rejected the request.
    Sql(QueryError),
}

impl From<IntegrityDeepError> for SqlIntegrityError {
    fn from(error: IntegrityDeepError) -> Self {
        Self::Integrity(error)
    }
}

impl From<IntegrityJobError> for SqlIntegrityError {
    fn from(error: IntegrityJobError) -> Self {
        Self::Integrity(IntegrityDeepError::Job(error))
    }
}

impl From<QueryError> for SqlIntegrityError {
    fn from(error: QueryError) -> Self {
        Self::Sql(error)
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one authorized administrative `CHECK INTEGRITY` statement.
    ///
    /// The caller must enforce controller or equivalent integrity-specific
    /// authorization before accepting caller-controlled SQL. `owner` must be
    /// the same stable identity used for typed Deep replay and abort.
    ///
    /// # Errors
    ///
    /// Returns [`SqlIntegrityError::Sql`] when grammar, entity resolution, or
    /// textual job identity lowering fails. Returns
    /// [`SqlIntegrityError::Integrity`] for canonical integrity protocol or
    /// engine failures.
    pub fn execute_admin_integrity_sql(
        &self,
        sql: &str,
        owner: IntegrityJobOwner,
    ) -> Result<IntegrityCheckResult, SqlIntegrityError> {
        let statement = parse_integrity_sql(sql).map_err(QueryError::from_sql_parse_error)?;
        let request = self.lower_integrity_sql_request(statement)?;

        self.execute_admin_integrity(request, owner)
            .map_err(SqlIntegrityError::from)
    }

    fn lower_integrity_sql_request(
        &self,
        statement: SqlIntegrityStatement,
    ) -> Result<IntegrityCheckRequest, SqlIntegrityError> {
        match statement {
            SqlIntegrityStatement::Quick { entity } => Ok(IntegrityCheckRequest::Quick {
                entity: self.integrity_sql_entity_selector(entity.as_str())?,
            }),
            SqlIntegrityStatement::DeepStart {
                entity,
                submission_key,
            } => Ok(IntegrityCheckRequest::DeepStart {
                entity: self.integrity_sql_entity_selector(entity.as_str())?,
                submission_key: IntegritySubmissionKey::new(submission_key)?,
            }),
            SqlIntegrityStatement::DeepContinue {
                job_id,
                acknowledged_sequence,
            } => Ok(IntegrityCheckRequest::deep_continue(
                IntegrityJobId::try_from_hex(job_id.as_str())?,
                acknowledged_sequence,
            )),
            SqlIntegrityStatement::DeepAbort { job_id } => Ok(IntegrityCheckRequest::deep_abort(
                IntegrityJobId::try_from_hex(job_id.as_str())?,
            )),
        }
    }

    fn integrity_sql_entity_selector(
        &self,
        sql_entity: &str,
    ) -> Result<IntegrityEntityIdentity, SqlIntegrityError> {
        let mut matched = None;
        for entity_registration in self.db.entity_registrations {
            let registration = entity_registration
                .runtime()
                .resolve(&self.db)
                .map_err(IntegrityDeepError::from)?;
            let store = self
                .db
                .recovered_store(registration.store_path)
                .map_err(IntegrityDeepError::from)?;
            let plan = self
                .accepted_inspection_plan_for_runtime_registration(registration, store)
                .map_err(|error| IntegrityDeepError::from(error.into_internal()))?;
            if !identifiers_tail_match(sql_entity, registration.entity_path)
                && !identifiers_tail_match(sql_entity, plan.snapshot().entity_name())
            {
                continue;
            }
            if matched.is_some() {
                return Err(QueryError::sql_lowering(SqlLoweringCode::EntityMismatch).into());
            }
            matched = Some(registration);
        }

        let registration = matched.ok_or_else(|| {
            SqlIntegrityError::from(QueryError::sql_lowering(SqlLoweringCode::EntityMismatch))
        })?;
        Ok(IntegrityEntityIdentity::from_runtime_selector(
            registration.entity_tag.value(),
            registration.entity_path,
            registration.store_path,
        ))
    }
}
