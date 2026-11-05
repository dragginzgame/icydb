//! Module: db::startup
//! Responsibility: derive bounded generated-database startup readiness.
//! Does not own: recovery execution, watchdog registration, or ordinary-operation admission.
//! Boundary: fixed durable controls plus runtime recovery witness -> readiness or typed failure.

mod driver;
mod observe;
pub(in crate::db) mod receipt;

use icydb_diagnostic_code::{
    Diagnostic, DiagnosticCode, DiagnosticDetail, DiagnosticFactTag, ErrorCode,
    MAX_PUBLIC_DIAGNOSTIC_FACTS, SchemaMigrationCode,
};
use serde::Deserialize;

use crate::{db::StoreRegistry, error::InternalError, traits::CanisterKind};

/// Current generated-database startup readiness.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum DatabaseStartupState {
    /// Recovery controls are complete and the generated schema is reconciled.
    Ready,
    /// Dedicated replicated startup work remains.
    Recovering,
}

/// One bounded outcome from the hidden replicated startup coordinator.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedStartupDriverStep {
    /// Startup is already ready or has an observable terminal failure for this boot.
    Terminal,
    /// Recovery or optional derived-evidence work remains after one bounded page.
    Recovering,
    /// Recovery is complete and generated schema reconciliation must run now.
    ApplyGeneratedSchema,
}

/// Closed owner of one terminal startup failure.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub enum StartupFailureKind {
    /// Database boot, incarnation, or commit-control failure.
    DatabaseControl,
    /// Journal-tail or fold-continuation recovery failure.
    JournalRecovery,
    /// Accepted-schema reconciliation or runtime preparation failure.
    SchemaReconciliation,
}

/// Internal bounded startup failure projected by the public facade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupFailure {
    kind: StartupFailureKind,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
}

impl StartupFailure {
    pub(in crate::db) fn from_internal(kind: StartupFailureKind, error: &InternalError) -> Self {
        Self::new(kind, error.diagnostic(), error.diagnostic_facts())
    }

    pub(in crate::db) fn new(
        kind: StartupFailureKind,
        diagnostic: Diagnostic,
        facts: Vec<(DiagnosticFactTag, u64)>,
    ) -> Self {
        debug_assert!(facts.len() <= MAX_PUBLIC_DIAGNOSTIC_FACTS);
        Self {
            kind,
            diagnostic,
            facts,
        }
    }

    /// Return the subsystem that owns this terminal startup failure.
    #[must_use]
    pub const fn kind(&self) -> StartupFailureKind {
        self.kind
    }

    /// Return its compact diagnostic identity.
    #[must_use]
    pub const fn diagnostic(&self) -> &Diagnostic {
        &self.diagnostic
    }

    /// Borrow its bounded numeric diagnostic facts.
    #[must_use]
    pub const fn facts(&self) -> &[(DiagnosticFactTag, u64)] {
        self.facts.as_slice()
    }
}

pub(in crate::db) fn classify_terminal_failure(
    kind: StartupFailureKind,
    error: &InternalError,
) -> Option<StartupFailure> {
    terminal_code_for_kind(kind, error.diagnostic().error_code())
        .then(|| StartupFailure::from_internal(kind, error))
}

pub(in crate::db) fn classify_terminal_failure_parts(
    kind: StartupFailureKind,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
) -> Option<StartupFailure> {
    terminal_code_for_kind(kind, diagnostic.error_code())
        .then(|| StartupFailure::new(kind, diagnostic, facts))
}

fn terminal_code_for_kind(
    kind: StartupFailureKind,
    code: icydb_diagnostic_code::ErrorCode,
) -> bool {
    // A runtime contradiction is evidence about code, not proof that persisted
    // state cannot recover under corrected code. Never memoize it as terminal.
    code != ErrorCode::STORE_INVARIANT_VIOLATION
        && code != ErrorCode::RUNTIME_INVARIANT_VIOLATION
        && receipt_code_for_kind(kind, code)
}

// Validate the existing receipt representation independently of current retry
// policy. Invariant receipts remain decodable so a corrected build can retire
// those diagnostic controls through successful recovery without discarding data.
fn receipt_code_for_kind(kind: StartupFailureKind, code: icydb_diagnostic_code::ErrorCode) -> bool {
    let common_receipt_code = code == ErrorCode::STORE_CORRUPTION
        || code == ErrorCode::STORE_INVARIANT_VIOLATION
        || code == ErrorCode::RUNTIME_CORRUPTION
        || code == ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT
        || code == ErrorCode::RUNTIME_INVARIANT_VIOLATION
        || code == ErrorCode::RUNTIME_BOUNDARY_PERSISTED_ROW_LAYOUT_OUTSIDE_ACCEPTED_WINDOW
        || code == ErrorCode::RUNTIME_BOUNDARY_PERSISTED_ROW_SLOT_COUNT_MISMATCH
        || code == ErrorCode::RUNTIME_BOUNDARY_ACCEPTED_ROW_CONSTRAINT_PROGRAM_CORRUPT;
    common_receipt_code
        || match kind {
            StartupFailureKind::DatabaseControl => false,
            StartupFailureKind::JournalRecovery => {
                code == ErrorCode::RUNTIME_BOUNDARY_JOURNAL_MUTATION_REVISION_EXHAUSTED
            }
            StartupFailureKind::SchemaReconciliation => {
                // The diagnostic registry owns migration categories. Planning
                // refusals and corrupt authority cannot progress by retrying;
                // leave controller lifecycle/publication conflicts unchanged.
                let migration_planning_failure = matches!(
                    code.diagnostic_detail(),
                    Some(DiagnosticDetail::SchemaMigration { reason })
                        if matches!(
                            reason.diagnostic_code(),
                            DiagnosticCode::RuntimeUnsupported | DiagnosticCode::RuntimeCorruption
                        ) || reason == SchemaMigrationCode::IdentityConflict
                );
                migration_planning_failure
                    || code == ErrorCode::SCHEMA_DDL_ADMISSION
                    || code == ErrorCode::RUNTIME_CONFLICT
                    || code == ErrorCode::RUNTIME_UNSUPPORTED
                    || code == ErrorCode::RUNTIME_BOUNDARY_GENERATED_FIELD_AFTER_DDL_FIELD
                    || code == ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
                    || code == ErrorCode::RUNTIME_BOUNDARY_GENERATED_CONSTRAINT_ACTIVATION_STALE
            }
        }
}

/// Observe one generated database without opening a request or advancing recovery.
pub fn observe_generated_startup_state<C: CanisterKind>(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<DatabaseStartupState, StartupFailure> {
    observe::observe::<C>(stores, submission_key)
}

/// Run at most one bounded recovery page without admitting ordinary work.
#[doc(hidden)]
pub fn drive_generated_startup_recovery_page<C: CanisterKind>(
    session: &crate::db::DbSession<C>,
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
) -> Result<GeneratedStartupDriverStep, InternalError> {
    driver::drive_recovery_page(session, stores, submission_key)
}

/// Persist one deterministic generated-schema failure against fresh authority.
#[doc(hidden)]
pub fn record_generated_schema_startup_failure<C: CanisterKind>(
    stores: &'static std::thread::LocalKey<StoreRegistry>,
    submission_key: &str,
    diagnostic: Diagnostic,
    facts: Vec<(DiagnosticFactTag, u64)>,
) -> Result<bool, InternalError> {
    driver::record_schema_failure::<C>(stores, submission_key, diagnostic, facts)
}

/// Clear a stale startup failure after an authoritative successful handoff.
#[doc(hidden)]
pub fn clear_generated_startup_failure<C: CanisterKind>() -> Result<bool, InternalError> {
    receipt::clear::<C>()
}

#[cfg(test)]
mod tests;
