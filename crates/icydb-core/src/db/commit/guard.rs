//! Module: db::commit::guard
//! Responsibility: enforce commit-window marker lifecycle and rollback guards.
//! Does not own: mutation planning, marker payload semantics, or recovery orchestration.
//! Boundary: executor::mutation -> commit::guard -> commit::store (one-way).

use crate::{
    db::commit::{
        PreparedRowCommitOp,
        marker::CommitMarker,
        store::{with_commit_store, with_commit_store_infallible},
    },
    error::InternalError,
};
use std::panic::{AssertUnwindSafe, catch_unwind};

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
        with_commit_store_infallible(super::store::CommitStore::clear_verified)
    }
}

/// Persist a commit marker and open the commit window.
pub(crate) fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
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
/// # Panics
///
/// Panics if successful commit completion does not clear the persisted marker,
/// or if failed commit completion does not preserve the persisted marker.
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
    if result.is_ok() {
        // Phase 1: successful apply must clear marker authority immediately.
        CommitGuard::clear()?;
        // Internal invariant: successful commit windows must clear the marker.
        if !with_commit_store_infallible(super::store::CommitStore::is_empty) {
            return Err(InternalError::commit_corruption());
        }
    } else {
        // Phase 1 (error path): failed apply must preserve marker authority.
        // Internal invariant: failed commit windows must preserve marker authority.
        if with_commit_store_infallible(super::store::CommitStore::is_empty) {
            return Err(InternalError::commit_corruption());
        }
    }

    result
}
