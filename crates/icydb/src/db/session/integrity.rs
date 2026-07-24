//! Module: db::session::integrity
//!
//! Responsibility: public trusted integrity-session facade and error mapping.
//! Does not own: authorization policy, inspection semantics, or durable jobs.
//! Boundary: caller-authorized typed request -> core integrity controller.

use crate::{db::session::DbSession, error::Error, traits::CanisterKind};

use candid::CandidType;
use icydb_core as core;
use serde::Deserialize;

/// Public failure from one trusted typed integrity request.

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum IntegrityCheckError {
    /// Accepted authority or physical execution failed.
    Database(Error),

    /// The bounded Deep job protocol rejected the request.
    Job(core::db::IntegrityJobError),
}

/// Public failure from administrative `CHECK INTEGRITY` SQL.
///
/// SQL grammar/lowering failures remain distinct from the canonical typed
/// integrity controller failures shared with the Rust request surface.

#[cfg(feature = "sql")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlIntegrityError {
    /// The canonical integrity controller rejected or could not execute the request.
    Integrity(IntegrityCheckError),

    /// SQL parsing or entity-name lowering rejected the request.
    Sql(Error),
}

impl From<core::db::IntegrityDeepError> for IntegrityCheckError {
    fn from(error: core::db::IntegrityDeepError) -> Self {
        match error {
            core::db::IntegrityDeepError::Job(error) => Self::Job(error),
            core::db::IntegrityDeepError::Internal(error) => Self::Database(error.into()),
        }
    }
}

#[cfg(feature = "sql")]
impl From<core::db::SqlIntegrityError> for SqlIntegrityError {
    fn from(error: core::db::SqlIntegrityError) -> Self {
        match error {
            core::db::SqlIntegrityError::Integrity(error) => {
                Self::Integrity(IntegrityCheckError::from(error))
            }
            core::db::SqlIntegrityError::Sql(error) => Self::Sql(error.into()),
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one trusted typed Quick or Deep integrity request.
    ///
    /// The caller must enforce controller or equivalent integrity-specific
    /// authorization before accepting caller-controlled requests. `owner`
    /// must identify that already-authorized caller or capability consistently
    /// across Deep start, continuation, replay, and abort.
    ///
    /// # Errors
    ///
    /// Returns [`IntegrityCheckError::Job`] for bounded protocol failures and
    /// [`IntegrityCheckError::Database`] when accepted authority or physical
    /// inspection cannot be read safely.
    pub fn execute_admin_integrity(
        &self,
        request: core::db::IntegrityCheckRequest,
        owner: core::db::IntegrityJobOwner,
    ) -> Result<core::db::IntegrityCheckResult, IntegrityCheckError> {
        self.inner
            .execute_admin_integrity(request, owner)
            .map_err(IntegrityCheckError::from)
    }

    /// Execute one authorized administrative `CHECK INTEGRITY` statement.
    ///
    /// The caller must enforce controller or equivalent integrity-specific
    /// authorization before accepting caller-controlled SQL. `owner` must
    /// identify that caller consistently across Deep operations.
    ///
    /// # Errors
    ///
    /// Returns [`SqlIntegrityError::Sql`] for SQL parsing and entity-lowering
    /// failures, or [`SqlIntegrityError::Integrity`] for the canonical typed
    /// integrity controller failures.
    #[cfg(feature = "sql")]
    pub fn execute_admin_integrity_sql(
        &self,
        sql: &str,
        owner: core::db::IntegrityJobOwner,
    ) -> Result<core::db::IntegrityCheckResult, SqlIntegrityError> {
        self.inner
            .execute_admin_integrity_sql(sql, owner)
            .map_err(SqlIntegrityError::from)
    }
}
