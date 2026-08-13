//! Module: db::commit::guard
//! Responsibility: enforce commit-window marker lifecycle and rollback guards.
//! Does not own: mutation planning, marker payload semantics, or recovery orchestration.
//! Boundary: executor::mutation -> commit::guard -> commit::store (one-way).

#[cfg(any(test, feature = "migration"))]
use crate::db::schema::preflight_schema_migration_record_op;
use crate::{
    db::{
        commit::{
            PreparedRowCommitOp,
            marker::{CommitMarker, DatabaseControlOp},
            store::{with_commit_store, with_initialized_commit_store},
        },
        integrity::preflight_mutation_progress_record_op,
        schema::preflight_schema_application_record_op,
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
};

thread_local! {
    // Generated canister lifecycle wiring installs the sole engine-owned
    // wake-up callback. Keeping the callback volatile makes its registration
    // atomic with a normally returned commit-apply failure while durable
    // recovery controls remain the only progress authority.
    static STARTUP_RECOVERY_WAKEUP: Cell<Option<fn()>> = const { Cell::new(None) };
}

/// Install the generated watchdog wake-up used by retained-marker error exits.
#[doc(hidden)]
pub fn install_startup_recovery_wakeup(wakeup: fn()) {
    STARTUP_RECOVERY_WAKEUP.with(|installed| installed.set(Some(wakeup)));
}

fn ensure_startup_recovery_wakeup() {
    STARTUP_RECOVERY_WAKEUP.with(|installed| {
        if let Some(wakeup) = installed.get() {
            wakeup();
        }
    });
}

///
/// ApplyRollback
///
/// Best-effort rollback payload owned by one in-flight apply guard.
/// This remains intentionally narrow:
/// - one closure for the batch rollback path
/// - one prepared row op for the single-row hot path
/// - no transactional semantics beyond "try to unwind local process state"
///

enum ApplyRollback {
    None,
    Closure(Box<dyn FnOnce()>),
    SinglePreparedRow(PreparedRowCommitOp),
}

///
/// CommitApplyGuard
///
/// Executor-internal guard for the commit-marker apply phase.
///
/// This guard is strictly best-effort infrastructure:
/// - Durable atomicity is owned by commit markers + recovery replay.
/// - Rollback closures here are best-effort, in-process cleanup only.
/// - This type does not provide transactional semantics or durable undo.
/// - New code must not rely on closure-based rollback for correctness.
///
/// Long-term direction:
/// marker application should become fully mechanical/idempotent so this guard
/// can be removed without changing user-visible correctness.
///

pub(crate) struct CommitApplyGuard {
    finished: bool,
    rollback: ApplyRollback,
}

impl CommitApplyGuard {
    /// Create one apply-phase rollback guard for diagnostic context `phase`.
    pub(crate) const fn new(_phase: &'static str) -> Self {
        Self {
            finished: false,
            rollback: ApplyRollback::None,
        }
    }

    pub(crate) fn record_rollback(&mut self, rollback: impl FnOnce() + 'static) {
        debug_assert!(
            matches!(self.rollback, ApplyRollback::None),
            "commit apply guard currently owns exactly one rollback closure",
        );

        if matches!(self.rollback, ApplyRollback::None) {
            self.rollback = ApplyRollback::Closure(Box::new(rollback));
        }
    }

    /// Record one prepared row-op rollback snapshot for the single-row hot path.
    pub(crate) fn record_single_row_rollback(&mut self, rollback: PreparedRowCommitOp) {
        debug_assert!(
            matches!(self.rollback, ApplyRollback::None),
            "commit apply guard currently owns exactly one rollback payload",
        );

        if matches!(self.rollback, ApplyRollback::None) {
            self.rollback = ApplyRollback::SinglePreparedRow(rollback);
        }
    }

    /// Mark the guarded apply phase complete and drop rollback closures.
    pub(crate) fn finish(mut self) -> Result<(), InternalError> {
        if self.finished {
            return Err(InternalError::executor_invariant());
        }

        self.finished = true;
        self.rollback = ApplyRollback::None;
        Ok(())
    }

    fn rollback_best_effort(&mut self) {
        if self.finished {
            // Defensive: rollback after finish is a logic error, but must not panic.
            return;
        }

        // Best-effort cleanup only:
        // - execute the one caller-owned rollback payload
        // - never unwind past this boundary
        match std::mem::replace(&mut self.rollback, ApplyRollback::None) {
            ApplyRollback::None => {}
            ApplyRollback::Closure(rollback) => {
                let _ = catch_unwind(AssertUnwindSafe(rollback));
            }
            ApplyRollback::SinglePreparedRow(rollback) => {
                let _ = catch_unwind(AssertUnwindSafe(|| rollback.apply()));
            }
        }
    }
}

impl Drop for CommitApplyGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.rollback_best_effort();
        }
    }
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Must not be leaked across mutation boundaries.
///

#[derive(Clone, Debug)]
pub(crate) struct CommitGuard;

impl CommitGuard {
    const fn new() -> Self {
        Self
    }

    /// Clear the commit marker after successful apply.
    fn clear() -> Result<(), InternalError> {
        with_initialized_commit_store(super::store::CommitStore::clear_verified)?
    }
}

/// Persist a commit marker and open the commit window.
pub(crate) fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    begin_commit_with_preflighted_mutation_progress(marker, false)
}

/// Persist one marker after preflighting its exact mutation-progress predecessor.
pub(in crate::db) fn begin_mutation_progress_commit<C: CanisterKind>(
    marker: CommitMarker,
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
    marker: CommitMarker,
    mutation_progress_preflighted: bool,
) -> Result<CommitGuard, InternalError> {
    for operation in marker.database_control() {
        match operation {
            DatabaseControlOp::SchemaApplication(operation) => {
                preflight_schema_application_record_op(operation)?;
            }
            #[cfg(any(test, feature = "migration"))]
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
    with_commit_store(|store| {
        // Phase 1: enforce one in-flight marker at a time before opening the
        // commit window.
        store.set_if_empty(&marker)?;

        Ok(CommitGuard::new())
    })
}

/// Apply commit ops and clear the marker only on successful completion.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// cleanup; durable authority remains the commit marker protocol.
///
/// Durability rule:
/// - `Ok(())` => marker is cleared.
/// - `Err(_)` => marker remains persisted for recovery replay.
///
pub(crate) fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply mutates stores from a prevalidated marker payload.
    // Marker durability + recovery replay remain the atomicity authority.
    // We only clear on success; failures keep the marker durable so recovery can
    // re-run the marker payload instead of losing commit authority.
    let result = apply(&mut guard);
    if let Err(error) = result {
        // A normally returned apply error commits its retained marker. Ensure
        // the recurring recovery wake-up in the same IC message before the
        // error is allowed to return; a trap rolls both changes back together.
        ensure_startup_recovery_wakeup();
        // Phase 1 (error path): failed apply must preserve marker authority.
        // Internal invariant: failed commit windows must preserve marker authority.
        if with_initialized_commit_store(super::store::CommitStore::is_empty)? {
            return Err(InternalError::commit_corruption());
        }
        return Err(error);
    }

    // Phase 1: successful apply must clear marker authority immediately. A
    // normally returned clear failure may retain the marker, so it must also
    // atomically retain a recovery wake-up.
    if let Err(error) = CommitGuard::clear() {
        ensure_startup_recovery_wakeup();
        return Err(error);
    }
    // Internal invariant: successful commit windows must clear the marker.
    let marker_is_empty = match with_initialized_commit_store(super::store::CommitStore::is_empty) {
        Ok(marker_is_empty) => marker_is_empty,
        Err(error) => {
            ensure_startup_recovery_wakeup();
            return Err(error);
        }
    };
    if !marker_is_empty {
        ensure_startup_recovery_wakeup();
        return Err(InternalError::commit_corruption());
    }

    Ok(())
}
