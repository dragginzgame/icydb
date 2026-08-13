//! Module: db::startup
//! Responsibility: public startup-readiness and bounded failure facade.
//! Does not own: recovery execution, persistence, or watchdog registration.
//! Boundary: core startup observation -> application-facing Candid-capable value.

use candid::CandidType;
use serde::Deserialize;

use crate::{Error, db::DatabaseBootstrapError};

#[doc(hidden)]
pub use icydb_core::db::GeneratedStartupDriverStep;
pub use icydb_core::db::{DatabaseStartupState, StartupFailureKind};

/// Bounded terminal failure returned by generated startup observation.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StartupFailure {
    kind: StartupFailureKind,
    diagnostic: Error,
}

impl StartupFailure {
    /// Return the subsystem that owns this terminal startup failure.
    #[must_use]
    pub const fn kind(&self) -> StartupFailureKind {
        self.kind
    }

    /// Return its compact diagnostic identity.
    #[must_use]
    pub fn diagnostic(&self) -> icydb_diagnostic_code::Diagnostic {
        self.diagnostic.diagnostic()
    }

    /// Borrow the bounded numeric diagnostic facts.
    #[must_use]
    pub const fn facts(&self) -> &[crate::DiagnosticFact] {
        self.diagnostic.facts()
    }

    /// Borrow the exact public diagnostic payload.
    #[must_use]
    pub const fn error(&self) -> &Error {
        &self.diagnostic
    }

    fn from_core(failure: icydb_core::db::StartupFailure) -> Self {
        Self {
            kind: failure.kind(),
            diagnostic: Error::from_diagnostic_and_facts(
                failure.diagnostic().clone(),
                failure.facts().to_vec(),
            ),
        }
    }
}

/// Observe generated startup controls without creating an ordinary session.
#[doc(hidden)]
pub fn __observe_generated_startup_state<C: icydb_core::traits::CanisterKind>(
    stores: &'static std::thread::LocalKey<icydb_core::db::StoreRegistry>,
    submission_key: &str,
) -> Result<DatabaseStartupState, StartupFailure> {
    icydb_core::db::observe_generated_startup_state::<C>(stores, submission_key)
        .map_err(StartupFailure::from_core)
}

/// Map memory-bootstrap failure into the bounded startup control result.
#[doc(hidden)]
#[must_use]
pub fn __startup_bootstrap_failure(error: DatabaseBootstrapError) -> StartupFailure {
    StartupFailure {
        kind: StartupFailureKind::DatabaseControl,
        diagnostic: Error::from(error),
    }
}

/// Memoize a deterministic generated-schema failure against fresh authority.
#[doc(hidden)]
pub fn __record_generated_schema_startup_failure<C: icydb_core::traits::CanisterKind>(
    stores: &'static std::thread::LocalKey<icydb_core::db::StoreRegistry>,
    submission_key: &str,
    error: &Error,
) -> Result<bool, Error> {
    let facts = error.core_facts().ok_or_else(|| {
        Error::from_kind(
            crate::ErrorKind::Runtime(crate::RuntimeErrorKind::InvariantViolation),
            crate::ErrorOrigin::Recovery,
        )
    })?;
    icydb_core::db::record_generated_schema_startup_failure::<C>(
        stores,
        submission_key,
        error.diagnostic(),
        facts,
    )
    .map_err(Into::into)
}

/// Clear any stale failure receipt after successful generated reconciliation.
#[doc(hidden)]
pub fn __clear_generated_startup_failure<C: icydb_core::traits::CanisterKind>()
-> Result<bool, Error> {
    icydb_core::db::clear_generated_startup_failure::<C>().map_err(Into::into)
}

/// Install the generated recovery wake-up at the commit-error boundary.
#[doc(hidden)]
pub fn __install_startup_recovery_wakeup(wakeup: fn()) {
    icydb_core::db::install_startup_recovery_wakeup(wakeup);
}

/// Build the dedicated retryable ordinary-admission error while startup is pending.
#[doc(hidden)]
#[must_use]
pub const fn __startup_recovery_pending() -> Error {
    Error::from_runtime_boundary(
        icydb_diagnostic_code::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
        crate::ErrorOrigin::Recovery,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{Decode, Encode};

    #[test]
    fn public_startup_failure_candid_shape_round_trips_exactly() {
        let failure = StartupFailure {
            kind: StartupFailureKind::JournalRecovery,
            diagnostic: Error::from_runtime_boundary(
                icydb_diagnostic_code::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
                crate::ErrorOrigin::Recovery,
            ),
        };
        let bytes = Encode!(&failure).expect("startup failure should encode");
        let decoded =
            Decode!(bytes.as_slice(), StartupFailure).expect("startup failure should decode");
        assert_eq!(decoded, failure);
        assert_eq!(decoded.kind(), StartupFailureKind::JournalRecovery);
        assert_eq!(
            decoded.error().code(),
            icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
        );
    }
}
