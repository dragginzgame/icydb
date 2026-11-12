//! Module: db::commit::guard
//! Responsibility: enforce commit-window marker lifecycle and rollback guards.
//! Does not own: mutation planning, marker payload semantics, or recovery orchestration.
//! Boundary: executor::mutation -> commit::guard -> commit::store (one-way).

// Test-only trap interception stays at the commit failure boundary; rollback state and helpers live in helper_tests.

#[cfg(any(test, feature = "migration"))]
use crate::db::schema::preflight_schema_migration_record_op;
use crate::{
    db::{
        commit::{
            BACKLOG_LIMITS, BacklogAdmission, ExactBacklogMeasurement, admit_backlog,
            current_database_backlog,
            marker::{CommitMarker, DatabaseControlOp},
            store::{EncodedCommitControlSlot, with_commit_store, with_initialized_commit_store},
        },
        integrity::preflight_mutation_progress_record_op,
        journal::JournalRecord,
        schema::preflight_schema_application_record_op,
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::cell::Cell;

#[cfg(test)]
use crate::db::commit::PreparedRowCommitOp;
#[cfg(test)]
use std::panic::{AssertUnwindSafe, catch_unwind};

#[cfg(not(test))]
const DEFAULT_STARTUP_RECOVERY_WAKEUP: Option<fn()> = None;

thread_local! {
    // Generated canister lifecycle wiring installs the sole engine-owned
    // wake-up callback. Keeping the callback volatile makes its registration
    // atomic with a normally returning commit that retains recovery work.
    // Durable recovery controls remain the only progress authority.
    static STARTUP_RECOVERY_WAKEUP: Cell<Option<fn()>> =
        const { Cell::new(DEFAULT_STARTUP_RECOVERY_WAKEUP) };
}

/// Install the generated watchdog wake-up for recovery and published schema work.
#[doc(hidden)]
pub fn install_startup_recovery_wakeup(wakeup: fn()) {
    STARTUP_RECOVERY_WAKEUP.with(|installed| installed.set(Some(wakeup)));
}

fn startup_recovery_wakeup() -> Result<fn(), InternalError> {
    STARTUP_RECOVERY_WAKEUP
        .with(Cell::get)
        .ok_or_else(InternalError::executor_invariant)
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Dropping an uncleared handle requests recovery through its captured callback.
/// Must not be leaked across mutation boundaries.
///

#[derive(Debug)]
pub(crate) struct CommitGuard {
    startup_recovery_wakeup: Option<fn()>,
    encoded_control_slot: EncodedCommitControlSlot,
    startup_work_pending: bool,
}

impl CommitGuard {
    const fn new(
        startup_recovery_wakeup: fn(),
        encoded_control_slot: EncodedCommitControlSlot,
        startup_work_pending: bool,
    ) -> Self {
        Self {
            startup_recovery_wakeup: Some(startup_recovery_wakeup),
            encoded_control_slot,
            startup_work_pending,
        }
    }

    /// Borrow one exact journal envelope already persisted in this marker.
    pub(crate) fn journal_batch_bytes(&self, ordinal: usize) -> Result<&[u8], InternalError> {
        self.encoded_control_slot.journal_batch_bytes(ordinal)
    }

    /// Clear the commit marker after successful apply.
    fn clear(&mut self) -> Result<(), InternalError> {
        with_initialized_commit_store(|store| {
            store.clear_live_verified(&self.encoded_control_slot)
        })??;
        // Schema publication also leaves startup work on heap stores: generated
        // reconciliation and replicated runtime preparation need the watchdog.
        // A failed clear always leaves the captured wake-up armed.
        if !self.startup_work_pending {
            self.startup_recovery_wakeup = None;
        }
        Ok(())
    }
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        if let Some(wakeup) = self.startup_recovery_wakeup.take() {
            wakeup();
        }
    }
}

/// Persist a commit marker and open the commit window.
pub(crate) fn begin_commit(marker: &CommitMarker) -> Result<CommitGuard, InternalError> {
    begin_commit_with_preflighted_mutation_progress(marker, false)
}

/// Persist one marker after preflighting its exact mutation-progress predecessor.
pub(in crate::db) fn begin_mutation_progress_commit<C: CanisterKind>(
    marker: &CommitMarker,
) -> Result<CommitGuard, InternalError> {
    let mut progress_count = 0_usize;
    for operation in marker.database_control() {
        if let DatabaseControlOp::MutationProgress(operation) = operation {
            progress_count = progress_count.saturating_add(1);
            preflight_mutation_progress_record_op::<C>(operation)?;
        }
    }
    if progress_count != 1 {
        return Err(InternalError::store_invariant());
    }
    begin_commit_with_preflighted_mutation_progress(marker, true)
}

fn begin_commit_with_preflighted_mutation_progress(
    marker: &CommitMarker,
    mutation_progress_preflighted: bool,
) -> Result<CommitGuard, InternalError> {
    for operation in marker.database_control() {
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                preflight_schema_application_record_op(operation)?;
            }
            DatabaseControlOp::EntitySourceLineage(operation) => {
                crate::db::schema::preflight_entity_source_lineage_catalog_op(operation)?;
            }
            #[cfg(any(test, feature = "migration"))]
            DatabaseControlOp::SchemaMigration(operation) => {
                preflight_schema_migration_record_op(operation)?;
            }
            DatabaseControlOp::MutationProgress(_) => {
                if !mutation_progress_preflighted {
                    return Err(InternalError::store_invariant());
                }
            }
        }
    }

    // Capture the generated wake-up before publishing marker authority. Every
    // normally returning commit path that can retain the marker must already
    // own a wake-up function; missing lifecycle wiring therefore fails before
    // any durable recovery work is created.
    let startup_recovery_wakeup = startup_recovery_wakeup()?;
    // Encode once without publishing marker authority so Gate 2 consumes the
    // exact envelopes later appended by the successful commit.
    let prepared = with_commit_store(|store| store.prepare_set_if_empty(marker))?;
    let proposed = ExactBacklogMeasurement::from_prepared_marker(marker, prepared.encoded())?;
    let current = current_database_backlog()?;
    match admit_backlog(current, proposed, BACKLOG_LIMITS)? {
        BacklogAdmission::Admitted { .. } => {}
        BacklogAdmission::Pressure(pressure) => {
            startup_recovery_wakeup();
            return Err(pressure.into_error());
        }
    }
    // Validated publication batches begin with their schema record. Unlike row
    // writes, even a zero-sequence heap publication needs a subsequent startup
    // page; journal backlog alone cannot express that obligation.
    let startup_work_pending = proposed.batch_count() != 0
        || marker.journal_batches().iter().any(|batch| {
            matches!(
                batch.records().first(),
                Some(JournalRecord::AcceptedSchemaPublish { .. })
            )
        });
    let encoded_control_slot = with_commit_store(|store| store.publish_prepared_marker(prepared))?;
    Ok(CommitGuard::new(
        startup_recovery_wakeup,
        encoded_control_slot,
        startup_work_pending,
    ))
}

/// Apply commit ops and clear the marker only on successful completion.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// cleanup; durable authority remains the commit marker protocol.
///
/// Successful application retires the marker. Unexpected application or
/// retirement failures terminate execution: an ordinary refusal must happen
/// before marker publication. Native fault fixtures may return errors to inspect
/// retained marker authority; that exception is absent from production.
///
pub(crate) fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply mutates stores from a prevalidated marker payload.
    // Marker durability + recovery replay remain the atomicity authority.
    // Keep every fallible post-marker operation inside one fatal-error boundary,
    // including marker observation and retirement, not only row application.
    let result = (|| {
        if let Err(error) = apply(&mut guard) {
            // Fault fixtures must retain recovery authority on returned errors.
            if with_initialized_commit_store(super::store::CommitStore::is_empty)? {
                return Err(InternalError::commit_corruption());
            }
            return Err(error);
        }

        // Retirement preflights its fallible checks before mechanical writes.
        // Pending startup work retains the guard's wake-up after success.
        guard.clear()
    })();
    enforce_preflighted_apply(result)
}

fn enforce_preflighted_apply(result: Result<(), InternalError>) -> Result<(), InternalError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            #[cfg(test)]
            if RETURN_COMMIT_FAILURES_FOR_TESTS.with(Cell::get) {
                return Err(error);
            }
            trap_preflighted_commit_apply_contradiction(error)
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn trap_preflighted_commit_apply_contradiction(_error: InternalError) -> ! {
    ic_platform_runtime::ic0::trap(
        b"preflighted commit application contradicted its complete preflight",
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn trap_preflighted_commit_apply_contradiction(_error: InternalError) -> ! {
    std::process::abort()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(crate) use helper_tests::CommitApplyGuard;
#[cfg(test)]
use helper_tests::DEFAULT_STARTUP_RECOVERY_WAKEUP;
#[cfg(test)]
use helper_tests::RETURN_COMMIT_FAILURES_FOR_TESTS;
