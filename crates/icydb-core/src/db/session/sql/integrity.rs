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
        sql::{
            identifier::identifiers_tail_match,
            parser::{SqlIntegrityStatement, parse_integrity_sql},
        },
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
    ) -> Result<IntegrityEntityIdentity, QueryError> {
        let mut matched = None;
        for hooks in self.db.entity_runtime_hooks {
            if !identifiers_tail_match(sql_entity, hooks.entity_path)
                && !identifiers_tail_match(sql_entity, hooks.model.name())
            {
                continue;
            }
            if matched.is_some() {
                return Err(QueryError::sql_lowering(SqlLoweringCode::EntityMismatch));
            }
            matched = Some(hooks);
        }

        let hooks =
            matched.ok_or_else(|| QueryError::sql_lowering(SqlLoweringCode::EntityMismatch))?;
        Ok(IntegrityEntityIdentity::from_runtime_selector(
            hooks.entity_tag.value(),
            hooks.entity_path,
            hooks.store_path,
        ))
    }
}
